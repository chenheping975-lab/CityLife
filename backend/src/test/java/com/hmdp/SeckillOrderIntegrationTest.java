package com.hmdp;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.dto.SeckillOrderResult;
import com.hmdp.dto.SeckillOrderStatusDTO;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SeckillOrderStatus;
import com.hmdp.utils.SeckillReservationHelper;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.hmdp.utils.RedisConstants.SECKILL_ACTIVITY_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_METRICS_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_RESERVATION_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 秒杀订单异步链路集成测试。
 *
 * <p>依赖真实 MySQL（127.0.0.1:3306/heima_dianping）、Redis（127.0.0.1:6380）与
 * RocketMQ（127.0.0.1:9876），运行前必须确保三项中间件可用，否则测试会失败或阻塞。
 * 测试数据使用当前时间戳派生的用户编号与自增券编号，结束后清理订单、券与 Redis key。</p>
 *
 * <p>类级 {@link Timeout} 兜底：中间件不可达时消费者可能阻塞，无超时会导致整个测试进程挂起。</p>
 *
 * <p>测试与在线实例隔离（{@code test} profile，见 {@code src/test/resources/application-test.yaml}）：
 * Redis 使用 DB 1，RocketMQ 使用 {@code seckill-order-topic-test} 与独立消费组，
 * 因此在线实例运行时也可以执行本测试，互不抢占消息、不污染线上指标。
 * 测试 Topic 需事先存在，可用 {@code scripts/init-test-topics.ps1} 幂等创建；
 * MySQL 仍共用 heima_dianping 库，测试数据在 {@code @AfterEach} 中清理。</p>
 */
@SpringBootTest
@ActiveProfiles("test")
@Timeout(300)
class SeckillOrderIntegrationTest {

    /** 测试用户编号基数，避免与本地已有用户冲突 */
    private static final long USER_BASE = 9_000_000L;

    private static final long POLL_INTERVAL_MS = 200L;

    /** 清理阶段重扫订单的最大轮次（覆盖在途消费者落库） */
    private static final int CLEANUP_ROUNDS = 5;
    /** 清理阶段每轮之间的间隔 */
    private static final long CLEANUP_INTERVAL_MS = 200L;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private IVoucherService voucherService;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private SeckillReservationHelper reservationHelper;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private PlatformTransactionManager transactionManager;

    /** 本次测试创建的优惠券编号，用于清理（仅测试线程写入，无需并发容器） */
    private final List<Long> voucherIds = new ArrayList<>();

    /**
     * 本次测试创建的订单编号，用于清理 Redis 预占记录。
     *
     * <p>原实现：{@code new ArrayList<>()}。修改原因：防超卖等并发用例由 64 个线程同时 add，
     * ArrayList 扩容期间写入会丢元素甚至抛 ArrayIndexOutOfBoundsException，
     * 造成预占记录清理不完整、断言漏检，测试结果不可信。</p>
     */
    private final Set<String> orderIds = ConcurrentHashMap.newKeySet();

    /**
     * 清理测试数据。
     *
     * <p>原实现：先按券号删两次订单（两次之间 {@code sleep(200)}），再删券与 Redis key，
     * 最后才删预占记录。修改原因：消费者是异步的，200ms 既不能保证消费者已处理完，
     * 也不能阻止删除窗口内落库的新订单，残留订单会污染后续用例。
     * 新顺序为“先删预占记录，再有界重扫订单”：预占记录是消费者落库的前置条件，
     * 删除后到达的消息会因缺少预占记录而快速失败，从源头停止继续产生订单。</p>
     */
    @AfterEach
    void cleanUp() {
        UserHolder.removeUser();
        clearReservations();
        if (!voucherIds.isEmpty()) {
            List<Long> ids = new ArrayList<>(voucherIds);
            // 仍在途的消费者可能已通过预占校验，故做有界重扫，而不是固定 sleep 后盲删一次
            for (int round = 0; round < CLEANUP_ROUNDS; round++) {
                if (round > 0) {
                    sleep(CLEANUP_INTERVAL_MS);
                }
                removeOrdersOfVouchers(ids);
                if (countOrdersOfVouchers(ids) == 0) {
                    break;
                }
            }
            seckillVoucherService.removeByIds(ids);
            voucherService.removeByIds(ids);
            for (Long voucherId : ids) {
                stringRedisTemplate.delete(SECKILL_ACTIVITY_KEY + voucherId);
                stringRedisTemplate.delete(SECKILL_STOCK_KEY + voucherId);
                stringRedisTemplate.delete(SECKILL_ORDER_KEY + voucherId);
            }
            voucherIds.clear();
        }
    }

    /**
     * 删除本次测试产生的预占记录：先删待处理集合成员，再删预占 Hash。
     */
    private void clearReservations() {
        for (String orderId : orderIds) {
            reservationHelper.removePending(orderId);
            stringRedisTemplate.delete(SECKILL_RESERVATION_KEY + orderId);
        }
        orderIds.clear();
    }

    private void removeOrdersOfVouchers(List<Long> ids) {
        voucherOrderService.remove(Wrappers.<VoucherOrder>lambdaQuery()
                .in(VoucherOrder::getVoucherId, ids));
    }

    private int countOrdersOfVouchers(List<Long> ids) {
        return voucherOrderService.query().in("voucher_id", ids).count();
    }

    @Test
    @DisplayName("US1：正常抢购返回 PROCESSING 与字符串订单号，异步落库后库存减一")
    void normalSeckillCreatesOrderAsynchronously() {
        long voterId = nextVoterId();
        Long voucherId = createSeckillVoucher(5, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().plusHours(1));

        Result accepted = asUser(voterId, () -> voucherOrderService.seckillVoucher(voucherId));
        assertTrue(accepted.getSuccess(), "抢购应被受理，实际：" + accepted.getErrorMsg());
        SeckillOrderResult result = (SeckillOrderResult) accepted.getData();
        assertNotNull(result.getOrderId());
        assertTrue(result.getOrderId().matches("\\d+"), "订单编号必须是纯数字字符串");
        assertEquals(SeckillOrderStatus.USER_PROCESSING, result.getStatus());
        orderIds.add(result.getOrderId());

        awaitAsync(() -> dbOrderCount(voucherId) == 1, 30_000,
                "异步订单未在超时时间内落库，voucherId=" + voucherId);

        assertEquals(4, dbStock(voucherId), "数据库库存应减一");
        assertEquals("4", redisStock(voucherId), "Redis 库存应减一");
        assertEquals(SeckillOrderStatus.SUCCESS, reservationStatus(result.getOrderId()));

        Result statusResult = asUser(voterId, () -> voucherOrderService.queryOrderStatus(result.getOrderId()));
        assertTrue(statusResult.getSuccess());
        SeckillOrderStatusDTO dto = (SeckillOrderStatusDTO) statusResult.getData();
        assertEquals(SeckillOrderStatus.USER_SUCCESS, dto.getStatus());
        assertNotNull(dto.getAcceptedAt());
    }

    @Test
    @DisplayName("US1：1000 并发争抢 100 库存，成功受理与最终订单数均为 100，库存不为负")
    void concurrentRequestsNeverOversell() throws Exception {
        Long voucherId = createSeckillVoucher(100, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().plusHours(1));
        int requests = 1000;
        int threads = 64;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(requests);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger stockEmpty = new AtomicInteger();
        Map<String, String> failures = new ConcurrentHashMap<>();

        long base = nextVoterId();
        for (int i = 0; i < requests; i++) {
            final long userId = base + i;
            pool.submit(() -> {
                try {
                    start.await();
                    Result result = asUser(userId, () -> voucherOrderService.seckillVoucher(voucherId));
                    if (Boolean.TRUE.equals(result.getSuccess())) {
                        accepted.incrementAndGet();
                        orderIds.add(((SeckillOrderResult) result.getData()).getOrderId());
                    } else if ("库存不足".equals(result.getErrorMsg())) {
                        stockEmpty.incrementAndGet();
                    } else {
                        failures.put(String.valueOf(userId), String.valueOf(result.getErrorMsg()));
                    }
                } catch (Exception e) {
                    failures.put(String.valueOf(userId), e.getClass().getSimpleName() + ":" + e.getMessage());
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(120, TimeUnit.SECONDS), "并发测试未在超时时间内完成");
        pool.shutdownNow();

        assertTrue(failures.isEmpty(), "出现非预期失败：" + failures);
        assertEquals(100, accepted.get(), "成功受理数必须等于库存");
        assertEquals(900, stockEmpty.get(), "其余请求必须返回库存不足");

        awaitAsync(() -> dbOrderCount(voucherId) == 100, 60_000,
                "异步订单未在超时时间内达到 100，当前=" + dbOrderCount(voucherId));

        assertEquals(0, dbStock(voucherId), "数据库库存必须归零且不为负");
        assertEquals("0", redisStock(voucherId), "Redis 库存必须归零");
        // 等待可能的重复消息处理完，确认没有多创建订单
        sleep(2_000);
        assertEquals(100, dbOrderCount(voucherId), "最终订单数必须等于库存，不得超卖");
    }

    @Test
    @DisplayName("US2：同一用户并发重复下单只受理一次，数据库只保留一单")
    void sameUserCanOnlyOrderOnce() throws Exception {
        Long voucherId = createSeckillVoucher(10, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().plusHours(1));
        long voterId = nextVoterId();

        int requests = 20;
        ExecutorService pool = Executors.newFixedThreadPool(requests);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(requests);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();

        for (int i = 0; i < requests; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    Result result = asUser(voterId, () -> voucherOrderService.seckillVoucher(voucherId));
                    if (Boolean.TRUE.equals(result.getSuccess())) {
                        accepted.incrementAndGet();
                        orderIds.add(((SeckillOrderResult) result.getData()).getOrderId());
                    } else if ("请勿重复下单".equals(result.getErrorMsg())
                            || "请求过于频繁，请稍后再试".equals(result.getErrorMsg())) {
                        // 同一用户 1 秒内超过限流阈值时先被限流拦截，结果一致：不产生第二单
                        duplicate.incrementAndGet();
                    }
                } catch (Exception ignored) {
                    // 竞争失败不计入断言
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(60, TimeUnit.SECONDS), "并发测试未在超时时间内完成");
        pool.shutdownNow();

        assertEquals(1, accepted.get(), "同一用户只能被受理一次");
        assertEquals(requests - 1, duplicate.get(), "其余请求必须被重复下单或限流拦截");

        awaitAsync(() -> dbOrderCount(voucherId) == 1, 30_000, "异步订单未落库");
        assertEquals(1, voucherOrderService.query().eq("user_id", voterId).eq("voucher_id", voucherId).count(),
                "数据库只能存在一单");
        assertEquals(9, dbStock(voucherId));
    }

    @Test
    @DisplayName("US1：库存耗尽的券返回库存不足")
    void stockEmptyIsRejected() {
        Long voucherId = createSeckillVoucher(1, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().plusHours(1));

        Result first = asUser(nextVoterId(), () -> voucherOrderService.seckillVoucher(voucherId));
        assertTrue(first.getSuccess());
        orderIds.add(((SeckillOrderResult) first.getData()).getOrderId());

        Result second = asUser(nextVoterId(), () -> voucherOrderService.seckillVoucher(voucherId));
        assertFalse(second.getSuccess());
        assertEquals("库存不足", second.getErrorMsg());
    }

    @Test
    @DisplayName("US1：活动开始前与结束后均拒绝受理")
    void activityWindowIsEnforced() {
        Long notStarted = createSeckillVoucher(5, LocalDateTime.now().plusHours(1), LocalDateTime.now().plusHours(2));
        Result beforeStart = asUser(nextVoterId(), () -> voucherOrderService.seckillVoucher(notStarted));
        assertFalse(beforeStart.getSuccess());
        assertEquals("活动尚未开始", beforeStart.getErrorMsg());

        Long ended = createSeckillVoucher(5, LocalDateTime.now().minusHours(2), LocalDateTime.now().minusHours(1));
        Result afterEnd = asUser(nextVoterId(), () -> voucherOrderService.seckillVoucher(ended));
        assertFalse(afterEnd.getSuccess());
        assertEquals("活动已结束", afterEnd.getErrorMsg());
    }

    @Test
    @DisplayName("US4：同一用户 1 秒内高频请求被限流")
    void rateLimitIsEnforced() throws Exception {
        Long voucherId = createSeckillVoucher(100, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().plusHours(1));
        long voterId = nextVoterId();
        int requests = 10;
        ExecutorService pool = Executors.newFixedThreadPool(requests);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(requests);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rateLimited = new AtomicInteger();

        for (int i = 0; i < requests; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    Result result = asUser(voterId, () -> voucherOrderService.seckillVoucher(voucherId));
                    if (Boolean.TRUE.equals(result.getSuccess())) {
                        accepted.incrementAndGet();
                        orderIds.add(((SeckillOrderResult) result.getData()).getOrderId());
                    } else if ("请求过于频繁，请稍后再试".equals(result.getErrorMsg())) {
                        rateLimited.incrementAndGet();
                    }
                } catch (Exception ignored) {
                    // 忽略竞争异常
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(60, TimeUnit.SECONDS), "并发测试未在超时时间内完成");
        pool.shutdownNow();

        assertEquals(1, accepted.get(), "同一用户只能被受理一次");
        assertTrue(rateLimited.get() >= 1, "高频请求必须触发限流，实际限流数=" + rateLimited.get());
    }

    @Test
    @DisplayName("US3：重复投递同一订单消息保持幂等，不重复扣库存")
    void duplicateMessageIsIdempotent() {
        long voterId = nextVoterId();
        Long voucherId = createSeckillVoucher(5, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().plusHours(1));

        Result accepted = asUser(voterId, () -> voucherOrderService.seckillVoucher(voucherId));
        assertTrue(accepted.getSuccess());
        String orderId = ((SeckillOrderResult) accepted.getData()).getOrderId();
        orderIds.add(orderId);

        awaitAsync(() -> dbOrderCount(voucherId) == 1, 30_000, "异步订单未落库");
        awaitAsync(() -> SeckillOrderStatus.SUCCESS.equals(reservationStatus(orderId)), 10_000, "预占未推进为 SUCCESS");

        SeckillOrderMessage message = new SeckillOrderMessage(1, orderId, voterId, voucherId, System.currentTimeMillis());
        voucherOrderService.createVoucherOrder(message);
        voucherOrderService.createVoucherOrder(message);

        assertEquals(1, dbOrderCount(voucherId), "重复消息不得重复创建订单");
        assertEquals(4, dbStock(voucherId), "重复消息不得重复扣减库存");
    }

    @Test
    @DisplayName("US2/US3：数据库唯一键冲突触发幂等补偿，释放 Redis 库存与购买资格")
    void uniqueKeyConflictTriggersCompensation() {
        long voterId = nextVoterId();
        Long voucherId = createSeckillVoucher(5, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().plusHours(1));

        // 直接构造“数据库已有该用户订单、Redis 无购买记录”的不一致场景
        VoucherOrder existing = new VoucherOrder();
        existing.setId(redisIdWorker.nextId("order"));
        existing.setUserId(voterId);
        existing.setVoucherId(voucherId);
        existing.setPayType(1);
        existing.setStatus(1);
        voucherOrderService.save(existing);

        Result accepted = asUser(voterId, () -> voucherOrderService.seckillVoucher(voucherId));
        assertTrue(accepted.getSuccess(), "受理阶段只校验 Redis，应当放行");
        String orderId = ((SeckillOrderResult) accepted.getData()).getOrderId();
        orderIds.add(orderId);

        awaitAsync(() -> SeckillOrderStatus.COMPENSATED.equals(reservationStatus(orderId)), 30_000,
                "唯一键冲突未触发补偿，当前状态=" + reservationStatus(orderId));

        assertEquals(5, Integer.parseInt(redisStock(voucherId)), "补偿后 Redis 库存必须回补");
        assertEquals(1, dbOrderCount(voucherId), "数据库中只保留原有订单");

        Result statusResult = asUser(voterId, () -> voucherOrderService.queryOrderStatus(orderId));
        assertTrue(statusResult.getSuccess());
        SeckillOrderStatusDTO dto = (SeckillOrderStatusDTO) statusResult.getData();
        assertEquals(SeckillOrderStatus.USER_FAILED, dto.getStatus());
        assertNotNull(dto.getReason());
    }

    @Test
    @DisplayName("US3：补偿幂等，重复补偿不会多次回补库存")
    void compensationIsIdempotent() {
        long voterId = nextVoterId();
        Long voucherId = createSeckillVoucher(5, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().plusHours(1));

        VoucherOrder existing = new VoucherOrder();
        existing.setId(redisIdWorker.nextId("order"));
        existing.setUserId(voterId);
        existing.setVoucherId(voucherId);
        existing.setPayType(1);
        existing.setStatus(1);
        voucherOrderService.save(existing);

        Result accepted = asUser(voterId, () -> voucherOrderService.seckillVoucher(voucherId));
        assertTrue(accepted.getSuccess());
        String orderId = ((SeckillOrderResult) accepted.getData()).getOrderId();
        orderIds.add(orderId);

        awaitAsync(() -> SeckillOrderStatus.COMPENSATED.equals(reservationStatus(orderId)), 30_000, "预占未补偿");
        String stockAfterCompensate = redisStock(voucherId);

        voucherOrderService.compensateOrder(orderId, "重复补偿校验");
        assertEquals(stockAfterCompensate, redisStock(voucherId), "重复补偿不得再次回补库存");
        assertEquals(SeckillOrderStatus.COMPENSATED, reservationStatus(orderId));
        assertFalse(reservationHelper.pendingOrders(System.currentTimeMillis() + 60_000, 1000).contains(orderId),
                "已补偿订单必须移出待处理集合");
    }

    @Test
    @DisplayName("US4：越权查询他人订单返回订单不存在")
    void crossUserQueryIsRejected() {
        long ownerId = nextVoterId();
        Long voucherId = createSeckillVoucher(5, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().plusHours(1));

        Result accepted = asUser(ownerId, () -> voucherOrderService.seckillVoucher(voucherId));
        assertTrue(accepted.getSuccess());
        String orderId = ((SeckillOrderResult) accepted.getData()).getOrderId();
        orderIds.add(orderId);
        awaitAsync(() -> dbOrderCount(voucherId) == 1, 30_000, "异步订单未落库");

        Result other = asUser(ownerId + 1, () -> voucherOrderService.queryOrderStatus(orderId));
        assertFalse(other.getSuccess());
        assertEquals("订单不存在", other.getErrorMsg());

        Result illegal = asUser(ownerId, () -> voucherOrderService.queryOrderStatus("abc123"));
        assertFalse(illegal.getSuccess());
        assertEquals("订单不存在", illegal.getErrorMsg());
    }

    @Test
    @DisplayName("US1：服务端身份的预占记录与请求受理时间可查询")
    void reservationRecordsServerSideIdentity() {
        long voterId = nextVoterId();
        Long voucherId = createSeckillVoucher(5, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().plusHours(1));

        long before = System.currentTimeMillis();
        Result accepted = asUser(voterId, () -> voucherOrderService.seckillVoucher(voucherId));
        assertTrue(accepted.getSuccess());
        String orderId = ((SeckillOrderResult) accepted.getData()).getOrderId();
        orderIds.add(orderId);

        assertEquals(voterId, reservationHelper.getUserId(orderId).longValue());
        assertEquals(voucherId, reservationHelper.getVoucherId(orderId));
        Long acceptedAt = reservationHelper.getAcceptedAt(orderId);
        assertNotNull(acceptedAt);
        assertTrue(acceptedAt >= before - 1000 && acceptedAt <= System.currentTimeMillis() + 1000,
                "受理时间必须来自服务端时钟");
    }

    @Test
    @DisplayName("US1：券创建事务回滚时不写入 Redis 活动元数据，避免幽灵活动")
    void activityRedisIsNotWrittenWhenTransactionRollsBack() {
        Voucher voucher = new Voucher()
                .setShopId(1L)
                .setTitle("回滚验证券")
                .setSubTitle("事务回滚不写 Redis")
                .setRules("每人限购一张")
                .setPayValue(100L)
                .setActualValue(200L)
                .setType(1)
                .setStatus(1)
                .setStock(5)
                .setBeginTime(LocalDateTime.now().minusMinutes(1))
                .setEndTime(LocalDateTime.now().plusHours(1));

        // 外层事务包裹新增操作后主动回滚，模拟“数据库提交失败”
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute(status -> {
            voucherService.addSeckillVoucher(voucher);
            assertNotNull(voucher.getId(), "事务内应已生成券编号");
            status.setRollbackOnly();
            return null;
        });

        Long voucherId = voucher.getId();
        assertNull(voucherService.getById(voucherId), "回滚后数据库不应存在该券");
        assertFalse(Boolean.TRUE.equals(stringRedisTemplate.hasKey(SECKILL_ACTIVITY_KEY + voucherId)),
                "回滚后不得写入 Redis 活动元数据");
        assertFalse(Boolean.TRUE.equals(stringRedisTemplate.hasKey(SECKILL_STOCK_KEY + voucherId)),
                "回滚后不得写入 Redis 库存");
    }

    @Test
    @DisplayName("回归：预占记录缺失时消费端改为告警丢弃，不再抛出异常触发 16 次重试")
    void missingReservationIsAckedInsteadOfRetried() {
        // 使用一个从未写入预占记录的订单编号，模拟“记录被删除/过期”或伪造消息
        String orderId = String.valueOf(redisIdWorker.nextId("order"));
        SeckillOrderMessage message = new SeckillOrderMessage(
                1, orderId, nextVoterId(), 1L, System.currentTimeMillis());

        long before = metricValue("reservationMissing");
        // 原实现会抛 IllegalStateException，被 RocketMQ 当作可重试错误，直到死信才停止
        assertDoesNotThrow(() -> voucherOrderService.createVoucherOrder(message),
                "预占记录缺失属永久性错误，不得再抛出可重试异常");
        // 指标与会话内的应用实例共享，存在并发写入，故只断言严格增长
        assertTrue(metricValue("reservationMissing") >= before + 1, "必须累计 reservationMissing 指标");
        assertNull(voucherOrderService.getById(Long.valueOf(orderId)), "缺失预占的消息不得创建订单");
        assertFalse(Boolean.TRUE.equals(stringRedisTemplate.hasKey(SECKILL_RESERVATION_KEY + orderId)),
                "消费端不得为缺失的预占记录新建键");
    }

    @Test
    @DisplayName("回归：订单编号非法的消息同样告警丢弃，不再抛出异常触发重试")
    void invalidOrderIdMessageIsAckedInsteadOfRetried() {
        SeckillOrderMessage message = new SeckillOrderMessage(
                1, "abc123", nextVoterId(), 1L, System.currentTimeMillis());

        long before = metricValue("invalidMessage");
        // 原实现会抛 IllegalArgumentException，被 RocketMQ 当作可重试错误
        assertDoesNotThrow(() -> voucherOrderService.createVoucherOrder(message),
                "订单编号非法属永久性错误，不得再抛出可重试异常");
        assertTrue(metricValue("invalidMessage") >= before + 1, "必须累计 invalidMessage 指标");
    }

    @Test
    @DisplayName("回归：对不存在的预占记录置 SUCCESS 返回 -3，且不得新建无 TTL 的空壳记录")
    void markSuccessOnMissingReservationDoesNotCreateKey() {
        String orderId = String.valueOf(redisIdWorker.nextId("order"));

        // 原实现返回 1 并 HSET 出一个 TTL = -1、只有 status/updatedAt 的空壳记录
        assertEquals(-3L, reservationHelper.markSuccess(orderId, System.currentTimeMillis()),
                "记录不存在时必须返回 -3");
        assertFalse(Boolean.TRUE.equals(stringRedisTemplate.hasKey(SECKILL_RESERVATION_KEY + orderId)),
                "不得新建空壳预占记录");
    }

    @Test
    @DisplayName("回归：历史空壳预占记录不影响本人已落库订单的状态查询")
    void shellReservationDoesNotBreakStatusQuery() {
        long ownerId = nextVoterId();
        Long voucherId = createSeckillVoucher(5, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().plusHours(1));

        // 手工构造旧版脚本可能遗留的空壳记录：只有 status/updatedAt，没有 userId，也没有 TTL
        String orderId = String.valueOf(redisIdWorker.nextId("order"));
        orderIds.add(orderId);
        Map<String, String> shell = new HashMap<>(2);
        shell.put("status", SeckillOrderStatus.SUCCESS);
        shell.put("updatedAt", String.valueOf(System.currentTimeMillis()));
        stringRedisTemplate.opsForHash().putAll(SECKILL_RESERVATION_KEY + orderId, shell);

        VoucherOrder order = new VoucherOrder();
        order.setId(Long.valueOf(orderId));
        order.setUserId(ownerId);
        order.setVoucherId(voucherId);
        order.setPayType(1);
        order.setStatus(1);
        voucherOrderService.save(order);

        Result own = asUser(ownerId, () -> voucherOrderService.queryOrderStatus(orderId));
        assertTrue(own.getSuccess(), "本人已落库订单必须可查询，实际：" + own.getErrorMsg());
        assertEquals(SeckillOrderStatus.USER_SUCCESS, ((SeckillOrderStatusDTO) own.getData()).getStatus());

        // 越权与不泄露归属的语义保持不变
        Result other = asUser(ownerId + 1, () -> voucherOrderService.queryOrderStatus(orderId));
        assertFalse(other.getSuccess());
        assertEquals("订单不存在", other.getErrorMsg());
    }

    // ------------------------------------------------------------------
    // 辅助方法
    // ------------------------------------------------------------------

    /**
     * 在指定用户身份下执行操作，结束后清理 ThreadLocal。
     */
    private <T> T asUser(long userId, Supplier<T> action) {
        UserDTO user = new UserDTO();
        user.setId(userId);
        user.setNickName("测试用户" + userId);
        UserHolder.saveUser(user);
        try {
            return action.get();
        } finally {
            UserHolder.removeUser();
        }
    }

    /**
     * 创建一张秒杀券并初始化 Redis 活动元数据与库存。
     */
    private Long createSeckillVoucher(int stock, LocalDateTime beginTime, LocalDateTime endTime) {
        Voucher voucher = new Voucher()
                .setShopId(1L)
                .setTitle("集成测试券")
                .setSubTitle("秒杀异步集成测试")
                .setRules("每人限购一张")
                .setPayValue(100L)
                .setActualValue(200L)
                .setType(1)
                .setStatus(1)
                .setStock(stock)
                .setBeginTime(beginTime)
                .setEndTime(endTime);
        voucherService.addSeckillVoucher(voucher);
        voucherIds.add(voucher.getId());
        return voucher.getId();
    }

    private int dbOrderCount(Long voucherId) {
        return voucherOrderService.query().eq("voucher_id", voucherId).count();
    }

    private int dbStock(Long voucherId) {
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        return voucher == null ? -1 : voucher.getStock();
    }

    private String redisStock(Long voucherId) {
        return stringRedisTemplate.opsForValue().get(SECKILL_STOCK_KEY + voucherId);
    }

    private String reservationStatus(String orderId) {
        return reservationHelper.getStatus(orderId);
    }

    /**
     * 读取当日秒杀指标当前值，用于断言告警指标。
     */
    private long metricValue(String metric) {
        Object value = stringRedisTemplate.opsForHash()
                .get(SECKILL_METRICS_KEY + LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")), metric);
        return value == null ? 0L : Long.parseLong(value.toString());
    }

    private long nextVoterId() {
        // 时间戳派生的用户编号，避免与历史测试数据冲突
        return USER_BASE + (System.nanoTime() % 10_000_000L);
    }

    /**
     * 轮询等待异步条件成立。
     */
    private void awaitAsync(Supplier<Boolean> condition, long timeoutMs, String message) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.get())) {
                return;
            }
            sleep(POLL_INTERVAL_MS);
        }
        if (!Boolean.TRUE.equals(condition.get())) {
            throw new AssertionError(message);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
