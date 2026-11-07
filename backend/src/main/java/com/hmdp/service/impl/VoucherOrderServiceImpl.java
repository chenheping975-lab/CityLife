package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.dto.SeckillOrderResult;
import com.hmdp.dto.SeckillOrderStatusDTO;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.SeckillOrderProducer;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SeckillActivityCache;
import com.hmdp.utils.SeckillMetrics;
import com.hmdp.utils.SeckillOrderStatus;
import com.hmdp.utils.SeckillReservationHelper;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private IVoucherService voucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    /**
     * 原实现（Redisson 用户锁）保留：旧同步下单链路使用，新链路改为 RocketMQ 异步创建 + 数据库唯一索引。
     */
    @Resource
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillActivityCache seckillActivityCache;

    @Resource
    private SeckillReservationHelper reservationHelper;

    @Resource
    private SeckillOrderProducer seckillOrderProducer;

    @Resource
    private SeckillMetrics seckillMetrics;

    @Resource
    private TransactionTemplate transactionTemplate;

    /** 每用户每活动每秒允许进入业务判断的请求次数 */
    @Value("${seckill.rate-limit.per-second:5}")
    private long rateLimitPerSecond;

    /** 活动元数据刷新间隔：60 秒，保证下架、改时间最多 1 分钟内生效 */
    private static final long ACTIVITY_REFRESH_INTERVAL_MS = 60_000L;

    /** 单订单锁获取重试次数：规避对账任务毫秒级的持锁窗口 */
    private static final int ORDER_LOCK_RETRY_TIMES = 3;

    /** 单订单锁获取重试间隔（毫秒） */
    private static final long ORDER_LOCK_RETRY_INTERVAL_MS = 100L;

    /**
     * 原实现（Redis Stream 方案）使用的受理脚本，保留原文件 seckill.lua 与加载逻辑，不做删除。
     */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    /** 新受理脚本：活动校验 + 限流 + 库存/一人一单 + 预占，单 Lua 原子完成 */
    private static final DefaultRedisScript<Long> SECKILL_ORDER_SCRIPT;

    /** 幂等补偿脚本 */
    private static final DefaultRedisScript<Long> SECKILL_COMPENSATE_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        SECKILL_ORDER_SCRIPT = new DefaultRedisScript<>();
        SECKILL_ORDER_SCRIPT.setLocation(new ClassPathResource("seckill_order.lua"));
        SECKILL_ORDER_SCRIPT.setResultType(Long.class);

        SECKILL_COMPENSATE_SCRIPT = new DefaultRedisScript<>();
        SECKILL_COMPENSATE_SCRIPT.setLocation(new ClassPathResource("seckill_compensate.lua"));
        SECKILL_COMPENSATE_SCRIPT.setResultType(Long.class);
    }

    /*
     * =====================================================================================
     * 原实现（Redis Stream 单消费者同步落库）保留，替换原因：
     * 1. Stream 消费者为 JVM 内单线程，宕机或阻塞时消息滞留，受理后可能长期无结果；
     * 2. 缺少可靠的发送确认与补偿闭环，无法保证“已受理必有着落”；
     * 3. 新方案改为 Lua 预占 + RocketMQ 异步创建 + 预占记录对账，原有代码不删除，按宪法要求注释保留。
     *
     * private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
     *
     * @PostConstruct
     * private void init() {
     *     SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
     * }
     *
     * private class VoucherOrderHandler implements Runnable {
     *
     *     @Override
     *     public void run() {
     *         while (true) {
     *             try {
     *                 // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
     *                 List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
     *                         Consumer.from("g1", "c1"),
     *                         StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
     *                         StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
     *                 );
     *                 // 2.判断订单信息是否为空
     *                 if (list == null || list.isEmpty()) {
     *                     continue;
     *                 }
     *                 // 解析数据
     *                 MapRecord<String, Object, Object> record = list.get(0);
     *                 Map<Object, Object> value = record.getValue();
     *                 VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
     *                 // 3.创建订单
     *                 createVoucherOrder(voucherOrder);
     *                 // 4.确认消息 XACK
     *                 stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
     *             } catch (Exception e) {
     *                 log.error("处理订单异常", e);
     *                 handlePendingList();
     *             }
     *         }
     *     }
     *
     *     private void handlePendingList() {
     *         while (true) {
     *             try {
     *                 List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
     *                         Consumer.from("g1", "c1"),
     *                         StreamReadOptions.empty().count(1),
     *                         StreamOffset.create("stream.orders", ReadOffset.from("0"))
     *                 );
     *                 if (list == null || list.isEmpty()) {
     *                     break;
     *                 }
     *                 MapRecord<String, Object, Object> record = list.get(0);
     *                 Map<Object, Object> value = record.getValue();
     *                 VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
     *                 createVoucherOrder(voucherOrder);
     *                 stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
     *             } catch (Exception e) {
     *                 log.error("处理订单异常", e);
     *             }
     *         }
     *     }
     * }
     *
     * private void createVoucherOrder(VoucherOrder voucherOrder) {
     *     Long userId = voucherOrder.getUserId();
     *     Long voucherId = voucherOrder.getVoucherId();
     *     RLock redisLock = redissonClient.getLock("lock:order:" + userId);
     *     boolean isLock = redisLock.tryLock();
     *     if (!isLock) {
     *         log.error("不允许重复下单！");
     *         return;
     *     }
     *     try {
     *         int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
     *         if (count > 0) {
     *             log.error("不允许重复下单！");
     *             return;
     *         }
     *         boolean success = seckillVoucherService.update()
     *                 .setSql("stock = stock - 1")
     *                 .eq("voucher_id", voucherId).gt("stock", 0)
     *                 .update();
     *         if (!success) {
     *             log.error("库存不足！");
     *             return;
     *         }
     *         save(voucherOrder);
     *     } finally {
     *         redisLock.unlock();
     *     }
     * }
     * =====================================================================================
     */

    /* 原实现（JVM 阻塞队列方案）保留：
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    createVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }
    */

    /**
     * 秒杀受理：Lua 原子完成活动状态/时间、限流、库存与一人一单校验并预占，
     * 随后异步发送 RocketMQ 订单创建事件，立即返回字符串订单编号与 PROCESSING。
     */
    @Override
    public Result  seckillVoucher(Long voucherId) {
        UserDTO user = UserHolder.getUser();
        if (user == null || user.getId() == null) {
            return Result.fail("请先登录");
        }
        if (voucherId == null || voucherId <= 0) {
            return Result.fail(SeckillOrderStatus.messageOf(SeckillOrderStatus.CODE_UNAVAILABLE));
        }
        Long userId = user.getId();

        // 1. 确保活动元数据存在（缓存未命中时从数据库回填，互斥防击穿）
        if (!ensureActivity(voucherId)) {
            log.warn("秒杀活动不存在或不可用，userId={}, voucherId={}", userId, voucherId);
            return Result.fail(SeckillOrderStatus.messageOf(SeckillOrderStatus.CODE_UNAVAILABLE));
        }

        // 2. 服务端生成订单编号与受理时间，客户端无法伪造
        String orderId = String.valueOf(redisIdWorker.nextId("order"));
        long acceptedAt = System.currentTimeMillis();
        int code;
        try {
            Long result = stringRedisTemplate.execute(
                    SECKILL_ORDER_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(),
                    userId.toString(),
                    orderId,
                    String.valueOf(acceptedAt),
                    String.valueOf(rateLimitPerSecond)
            );
            code = result == null ? SeckillOrderStatus.CODE_UNAVAILABLE : result.intValue();
        } catch (Exception e) {
            log.error("秒杀受理脚本执行异常，userId={}, voucherId={}, orderId={}", userId, voucherId, orderId, e);
            return Result.fail("服务器异常");
        }

        // 3. 返回码映射与指标
        if (code != SeckillOrderStatus.CODE_SUCCESS) {
            if (code == SeckillOrderStatus.CODE_DUPLICATE) {
                seckillMetrics.increment("duplicate");
            } else if (code == SeckillOrderStatus.CODE_RATE_LIMITED) {
                seckillMetrics.increment("rateLimited");
            }
            log.info("秒杀受理被拒绝，code={}, userId={}, voucherId={}, orderId={}",
                    code, userId, voucherId, orderId);
            return Result.fail(SeckillOrderStatus.messageOf(code));
        }

        seckillMetrics.increment("accepted");
        log.info("秒杀受理成功，orderId={}, userId={}, voucherId={}", orderId, userId, voucherId);

        // 4. 发送订单创建事件；发送失败保留 RESERVED，由对账任务重发
        SeckillOrderMessage message = new SeckillOrderMessage(1, orderId, userId, voucherId, acceptedAt);
        boolean sent = seckillOrderProducer.send(message);
        // 原实现：直接在方法内推进预占状态，未做异常兜底。
        // 修改原因：此时受理 Lua 已扣库存、占购买资格，消息也可能已发出，订单必然创建。
        // 若 markSent 因 Redis 抖动抛异常，异常会穿出到全局异常处理器，用户看到“服务器异常”，
        // 但订单仍会落库，用户重试又得到“请勿重复下单”，出现“报错却下单成功”的错觉。
        // 因此受理提交后的状态推进失败只记录日志：预占保持 RESERVED，由对账任务按订单号幂等重发。
        try {
            if (sent) {
                if (!reservationHelper.markSent(orderId, System.currentTimeMillis())) {
                    log.warn("订单事件已发送但预占状态未推进为 SENT，orderId={}", orderId);
                }
            } else {
                log.warn("订单事件发送失败，预占保持 RESERVED，等待对账重发，orderId={}", orderId);
            }
        } catch (RuntimeException e) {
            log.warn("预占状态推进失败，由对账任务修复，orderId={}", orderId, e);
        }

        // 5. 已受理：返回字符串订单编号与处理中状态
        return Result.ok(new SeckillOrderResult(orderId, SeckillOrderStatus.USER_PROCESSING));
    }

    /* 原实现（Redis Stream + Lua 预占）保留，替换原因见上方说明：
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 3.返回订单id
        return Result.ok(orderId);
    }
    */

    /**
     * 消费订单创建事件：以 orderId 为幂等键，在事务内条件扣减数据库库存并插入订单。
     *
     * <p>异常按“是否可自愈”分为三类：</p>
     * <ul>
     *     <li>业务冲突（唯一键冲突、数据库库存不足）：就地幂等补偿，不重试；</li>
     *     <li>消息或预占数据本身不合法（编号非法、预占记录缺失/不完整）：永久性错误，
     *         告警并累计指标后 ack 丢弃，重试无意义；</li>
     *     <li>基础设施异常（Redis/MySQL 不可用、锁长期被占）：向上抛出，交由 RocketMQ 重试直至死信。</li>
     * </ul>
     */
    @Override
    public void createVoucherOrder(SeckillOrderMessage message) {
        // 原实现：消息不合法时抛 IllegalArgumentException，交由 RocketMQ 重试。
        // 修改原因：消息本身不合法（缺少订单编号、编号非数字）属于永久性错误，重试不可能自愈；
        // 抛出会让消息重试 16 次（约 4 小时 46 分）后才进入死信，期间反复打印异常栈并占用消费线程。
        // 改为告警 + 指标后直接返回，由容器 ack 丢弃。注意：JSON 解析失败仍保持可重试，
        // 因为滚动发布期间生产者与消费者版本不一致属于临时状态，重试窗口可以跨过发布。
        if (message == null || message.getOrderId() == null) {
            log.error("秒杀订单消息缺少订单编号，消息不可重试，已丢弃，message={}", message);
            seckillMetrics.increment("invalidMessage");
            return;
        }
        String orderId = message.getOrderId();
        Long orderIdValue = parseOrderId(orderId);
        if (orderIdValue == null) {
            log.error("订单编号不合法，消息不可重试，已丢弃，orderId={}", orderId);
            seckillMetrics.increment("invalidMessage");
            return;
        }

        // 与补偿共用单订单锁，避免“补偿释放库存”与“订单落库”并发交错
        // 原实现：一次获取失败立即抛异常，依赖 RocketMQ 一级重试（默认约 10 秒）。
        // 修改原因：对账任务在同一订单锁内评估并发送消息，持锁窗口只有毫秒级；
        // 立即抛异常会让每一笔由对账重发的订单都白白等待一个重试周期，并消耗重试次数。
        // 短暂重试可覆盖该窗口，仍失败时再交给 RocketMQ。
        String lockToken = tryLockOrderWithRetry(orderId);
        if (lockToken == null) {
            throw new IllegalStateException("订单正在处理中，orderId=" + orderId);
        }
        try {
            // 1. 幂等：订单已存在，直接确认
            if (getById(orderIdValue) != null) {
                markReservationSuccess(orderId);
                log.info("订单已存在，幂等确认，orderId={}", orderId);
                return;
            }

            // 2. 预占记录校验
            String reservationStatus = reservationHelper.getStatus(orderId);
            if (reservationStatus == null) {
                // 原实现：
                //   log.warn("预占记录缺失，orderId={}", orderId);
                //   throw new IllegalStateException("预占记录缺失，orderId=" + orderId);
                // 修改原因：预占记录由受理 Lua 在发送消息之前原子写入，TTL 不少于 30 天，且消息只可能
                // 来自“受理成功后的同步发送”或“对账任务的重发（重发读取的正是该预占记录）”。
                // 因此“记录不存在”只可能来自记录被删除/过期或伪造消息，属于永久性错误，重试无法自愈。
                // 抛出会消耗 16 次重试（约 4 小时 46 分）才进入死信，期间反复打印日志、占用消费线程；
                // Broker 未开启 ACL 时，伪造消息还能借此放大为消费线程耗尽。
                // 改为告警 + 指标后 ack 丢弃：此时没有预占记录可补偿，也不会创建订单。
                // 严禁用消息体中的 userId/voucherId 去释放库存或购买资格，那正是伪造消息取消他人订单的攻击面。
                log.error("预占记录缺失，消息不可重试，已丢弃，orderId={}", orderId);
                seckillMetrics.increment("reservationMissing");
                return;
            }
            if (SeckillOrderStatus.COMPENSATED.equals(reservationStatus)) {
                log.warn("预占已补偿，不再创建订单，orderId={}", orderId);
                return;
            }
            if (SeckillOrderStatus.SUCCESS.equals(reservationStatus)) {
                log.info("预占已是 SUCCESS，幂等确认，orderId={}", orderId);
                return;
            }

            // 原实现（消息身份优先，预占记录仅兜底）：
            // Long userId = message.getUserId() != null ? message.getUserId() : reservationHelper.getUserId(orderId);
            // Long voucherId = message.getVoucherId() != null ? message.getVoucherId() : reservationHelper.getVoucherId(orderId);
            // 修改原因：消息体可被伪造（Broker 未开启 ACL），而下单身份必须由服务端决定；
            // 预占记录由受理 Lua 原子写入，是唯一可信来源。消息中的 userId/voucherId 降级为一致性校验，
            // 不一致视为伪造消息，直接丢弃（ack）并告警，绝不触发补偿——否则伪造消息会让合法预占
            // 进入死信并被补偿，攻击者可借此取消真实用户的订单。
            Long userId = reservationHelper.getUserId(orderId);
            Long voucherId = reservationHelper.getVoucherId(orderId);
            if (userId == null || voucherId == null) {
                // 原实现：throw new IllegalArgumentException("预占记录缺少 userId/voucherId，orderId=" + orderId);
                // 修改原因：同上，预占记录字段缺失属于数据损坏，重试无法修复；
                // 且此时没有可信身份可用于补偿，只能告警 + ack 丢弃。
                log.error("预占记录缺少 userId/voucherId，消息不可重试，已丢弃，orderId={}", orderId);
                seckillMetrics.increment("reservationMissing");
                return;
            }
            if ((message.getUserId() != null && !message.getUserId().equals(userId))
                    || (message.getVoucherId() != null && !message.getVoucherId().equals(voucherId))) {
                log.error("秒杀订单消息身份与预占记录不一致，疑似伪造，已丢弃，orderId={}, "
                                + "messageUserId={}, messageVoucherId={}, reservationUserId={}, reservationVoucherId={}",
                        orderId, message.getUserId(), message.getVoucherId(), userId, voucherId);
                return;
            }

            try {
                transactionTemplate.executeWithoutResult(transactionStatus -> {
                    // 一人一单预检查：唯一索引是最终防线，这里提前给出明确失败原因
                    int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
                    if (count > 0) {
                        throw new DuplicateUserOrderException("用户已存在该活动订单");
                    }
                    // 数据库条件扣减：stock > 0 保证不出现负库存
                    boolean success = seckillVoucherService.update()
                            .setSql("stock = stock - 1")
                            .eq("voucher_id", voucherId)
                            .gt("stock", 0)
                            .update();
                    if (!success) {
                        throw new StockNotEnoughException("数据库库存不足");
                    }
                    VoucherOrder voucherOrder = new VoucherOrder();
                    voucherOrder.setId(orderIdValue);
                    voucherOrder.setUserId(userId);
                    voucherOrder.setVoucherId(voucherId);
                    voucherOrder.setPayType(1);
                    voucherOrder.setStatus(1);
                    save(voucherOrder);
                });
            } catch (DuplicateKeyException e) {
                // 并发下同一 orderId 或同一 (user_id, voucher_id) 冲突
                if (getById(orderIdValue) != null) {
                    markReservationSuccess(orderId);
                    log.info("唯一键冲突但订单已落库，按幂等成功处理，orderId={}", orderId);
                    return;
                }
                seckillMetrics.increment("duplicate");
                log.warn("唯一键冲突，触发补偿，orderId={}, userId={}, voucherId={}", orderId, userId, voucherId);
                compensateOrderLocked(orderId, "用户已存在该活动订单");
                seckillMetrics.increment("failed");
                return;
            } catch (DuplicateUserOrderException e) {
                seckillMetrics.increment("duplicate");
                log.warn("用户已存在该活动订单，触发补偿，orderId={}, userId={}, voucherId={}", orderId, userId, voucherId);
                compensateOrderLocked(orderId, e.getMessage());
                seckillMetrics.increment("failed");
                return;
            } catch (StockNotEnoughException e) {
                log.error("数据库库存不足，触发补偿，orderId={}, userId={}, voucherId={}", orderId, userId, voucherId);
                compensateOrderLocked(orderId, "库存不足");
                seckillMetrics.increment("failed");
                return;
            }

            // 3. 事务提交后推进预占状态；失败不影响订单正确性，由对账任务修复
            markReservationSuccess(orderId);
            seckillMetrics.increment("created");
            log.info("秒杀订单创建成功，orderId={}, userId={}, voucherId={}", orderId, userId, voucherId);
        } finally {
            reservationHelper.unlockOrder(orderId, lockToken);
        }
    }

    /**
     * 获取单订单锁，短暂重试以规避对账任务毫秒级的持锁窗口。
     *
     * <p>对账任务在同一把锁内完成“评估 + 发送 + markSent”，持锁时间通常只有几十毫秒；
     * 直接抛异常会让订单等待 RocketMQ 一级重试（约 10 秒）。重试耗尽后仍交给 RocketMQ，
     * 保证锁被长期占用（如实例卡死）时不会无限阻塞消费线程。</p>
     *
     * @return 锁 token；重试耗尽仍失败返回 null
     */
    private String tryLockOrderWithRetry(String orderId) {
        String token = reservationHelper.tryLockOrder(orderId);
        for (int i = 0; token == null && i < ORDER_LOCK_RETRY_TIMES; i++) {
            try {
                Thread.sleep(ORDER_LOCK_RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            token = reservationHelper.tryLockOrder(orderId);
        }
        return token;
    }

    /**
     * 状态查询：先查订单，再查预占记录；仅本人可查询，其他情况统一返回“订单不存在”。
     */
    @Override
    public Result queryOrderStatus(String orderId) {
        UserDTO user = UserHolder.getUser();
        if (user == null || user.getId() == null) {
            return Result.fail("请先登录");
        }
        Long orderIdValue = parseOrderId(orderId);
        if (orderIdValue == null) {
            return Result.fail("订单不存在");
        }
        Long currentUserId = user.getId();

        VoucherOrder order = getById(orderIdValue);
        Map<Object, Object> reservation = reservationHelper.get(orderId);
        // 原实现：先判断 reservation == null 走数据库兜底；再由 ownerId == null 一律返回“订单不存在”。
        // 修改原因：旧版 MARK_SUCCESS 脚本在预占记录缺失时会 HSET 出一个只有 status/updatedAt、
        // 没有 userId 且无 TTL 的空壳记录。它会跳过“记录不存在”的兜底分支，
        // 随后因 ownerId == null 被拒绝，导致“订单已落库且属于本人”的用户轮询状态时一直看到
        // “订单不存在”。空壳记录与记录缺失本质一致，统一以数据库为准。
        Long ownerId = reservation == null ? null : reservationHelper.getUserId(orderId);
        if (ownerId == null) {
            // 预占记录缺失或不完整：仅当订单确实属于当前用户时返回成功
            if (order != null && currentUserId.equals(order.getUserId())) {
                return Result.ok(new SeckillOrderStatusDTO(
                        orderId,
                        SeckillOrderStatus.USER_SUCCESS,
                        null,
                        toEpochMillis(order.getCreateTime())));
            }
            return Result.fail("订单不存在");
        }
        if (!ownerId.equals(currentUserId)) {
            // 不泄露订单是否存在及归属
            return Result.fail("订单不存在");
        }

        String reservationStatus = reservationHelper.getStatus(orderId);
        String userStatus = SeckillOrderStatus.toUserStatus(reservationStatus);
        String reason = null;
        if (SeckillOrderStatus.USER_FAILED.equals(userStatus)) {
            reason = reservationHelper.getFailReason(orderId);
            if (reason == null || reason.isEmpty()) {
                reason = "订单创建失败";
            }
        }
        // 订单已落库时以数据库为准
        if (order != null) {
            userStatus = SeckillOrderStatus.USER_SUCCESS;
            reason = null;
        }
        return Result.ok(new SeckillOrderStatusDTO(
                orderId,
                userStatus,
                reason,
                reservationHelper.getAcceptedAt(orderId)));
    }

    /**
     * 幂等补偿：获取单订单锁后释放库存与购买资格。
     */
    @Override
    public void compensateOrder(String orderId, String reason) {
        if (parseOrderId(orderId) == null) {
            log.warn("补偿参数不合法，orderId={}", orderId);
            return;
        }
        String lockToken = reservationHelper.tryLockOrder(orderId);
        if (lockToken == null) {
            // 其他实例正在处理：保留待处理集合记录，等下次对账重试
            log.warn("补偿未获取到订单锁，等待下次对账，orderId={}", orderId);
            return;
        }
        try {
            compensateOrderLocked(orderId, reason);
        } finally {
            reservationHelper.unlockOrder(orderId, lockToken);
        }
    }

    /**
     * 已持有单订单锁时执行补偿。
     */
    private void compensateOrderLocked(String orderId, String reason) {
        Long orderIdValue = parseOrderId(orderId);
        // 订单已落库时绝不能补偿，否则会把有效订单的库存释放掉
        if (orderIdValue != null && getById(orderIdValue) != null) {
            markReservationSuccess(orderId);
            log.warn("订单已存在，取消补偿并修复预占状态为 SUCCESS，orderId={}", orderId);
            return;
        }
        Long voucherId = reservationHelper.getVoucherId(orderId);
        Long userId = reservationHelper.getUserId(orderId);
        if (voucherId == null || userId == null) {
            log.error("预占记录不完整，无法补偿，orderId={}", orderId);
            reservationHelper.removePending(orderId);
            return;
        }
        String failReason = (reason == null || reason.isEmpty()) ? "订单创建失败" : reason;
        Long result = stringRedisTemplate.execute(
                SECKILL_COMPENSATE_SCRIPT,
                Collections.emptyList(),
                orderId,
                voucherId.toString(),
                userId.toString(),
                failReason,
                String.valueOf(System.currentTimeMillis())
        );
        int code = result == null ? 2 : result.intValue();
        if (code == 0) {
            seckillMetrics.increment("compensated");
            log.warn("秒杀预占补偿完成，orderId={}, userId={}, voucherId={}, reason={}",
                    orderId, userId, voucherId, failReason);
        } else if (code == 1) {
            log.info("预占已处于终态，无需补偿，orderId={}", orderId);
        } else {
            log.error("预占记录不存在，无法补偿，orderId={}", orderId);
            reservationHelper.removePending(orderId);
        }
    }

    /**
     * 推进预占状态为 SUCCESS 并移出待处理集合。
     */
    private void markReservationSuccess(String orderId) {
        long result = reservationHelper.markSuccess(orderId, System.currentTimeMillis());
        if (result == -1L) {
            // 正常情况下单订单锁可避免该冲突；出现说明锁过期或人工干预，必须人工核对
            log.error("预占记录已补偿但订单已创建，需人工核对，orderId={}", orderId);
        } else if (result == -3L) {
            // 原实现：预占脚本在记录缺失时仍会 HSET 出一个无 TTL 的空壳记录并返回 1，
            // 使本分支不可达，故障被静默。修改原因：脚本增加 EXISTS 守卫后返回 -3，
            // “订单已落库但预占记录丢失”这一不一致必须告警，否则只能靠用户投诉发现。
            log.error("订单已创建但预占记录缺失，需人工核对，orderId={}", orderId);
            seckillMetrics.increment("reservationMissing");
        }
        reservationHelper.removePending(orderId);
    }

    /**
     * 确保活动元数据存在；缓存未命中时从 tb_voucher + tb_seckill_voucher 回填，SETNX 互斥防击穿。
     */
    private boolean ensureActivity(Long voucherId) {
        // 原实现：只判断活动元数据是否存在（exists），写入后永不更新。
        // 修改原因：活动下架、调整起止时间后，Redis 中的旧元数据会一直生效（TTL 至少 30 天），
        // 导致已下架活动仍可抢购。改为按 ACTIVITY_REFRESH_INTERVAL_MS 判断新鲜度，
        // 过期后从数据库回刷 status/beginTime/endTime（initialStock 保持不变）。
        if (seckillActivityCache.isFresh(voucherId, ACTIVITY_REFRESH_INTERVAL_MS)) {
            return true;
        }
        String lockToken = seckillActivityCache.tryLock(voucherId);
        if (lockToken == null) {
            // 其他线程正在回填，最多等待 500ms
            for (int i = 0; i < 10; i++) {
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                if (seckillActivityCache.isFresh(voucherId, ACTIVITY_REFRESH_INTERVAL_MS)) {
                    return true;
                }
            }
            return seckillActivityCache.isFresh(voucherId, ACTIVITY_REFRESH_INTERVAL_MS);
        }
        try {
            if (seckillActivityCache.isFresh(voucherId, ACTIVITY_REFRESH_INTERVAL_MS)) {
                return true;
            }
            Voucher voucher = voucherService.getById(voucherId);
            if (voucher == null || voucher.getType() == null || voucher.getType() != 1) {
                return false;
            }
            SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
            if (seckillVoucher == null || seckillVoucher.getBeginTime() == null || seckillVoucher.getEndTime() == null) {
                return false;
            }
            long beginTime = toEpochMillis(seckillVoucher.getBeginTime());
            long endTime = toEpochMillis(seckillVoucher.getEndTime());
            int status = voucher.getStatus() == null ? 1 : voucher.getStatus();
            int initialStock = seckillVoucher.getStock() == null ? 0 : seckillVoucher.getStock();
            seckillActivityCache.save(voucherId, beginTime, endTime, status, initialStock);
            // 兼容功能上线前创建的活动：库存 key 缺失时用数据库库存初始化
            stringRedisTemplate.opsForValue().setIfAbsent(
                    SECKILL_STOCK_KEY + voucherId,
                    String.valueOf(initialStock),
                    seckillActivityCache.ttlSeconds(endTime),
                    TimeUnit.SECONDS);
            log.info("秒杀活动元数据回填/刷新完成，voucherId={}, beginTime={}, endTime={}, status={}, initialStock={}",
                    voucherId, beginTime, endTime, status, initialStock);
            return true;
        } finally {
            seckillActivityCache.unlock(voucherId, lockToken);
        }
    }

    /**
     * 订单编号合法性校验：仅允许 1-19 位数字，避免数值溢出与非数字注入。
     */
    private Long parseOrderId(String orderId) {
        if (orderId == null || orderId.isEmpty() || orderId.length() > 19) {
            return null;
        }
        for (int i = 0; i < orderId.length(); i++) {
            if (!Character.isDigit(orderId.charAt(i))) {
                return null;
            }
        }
        try {
            return Long.valueOf(orderId);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private long toEpochMillis(LocalDateTime dateTime) {
        return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * 数据库库存不足（不可重试，触发补偿）。
     */
    private static class StockNotEnoughException extends RuntimeException {
        StockNotEnoughException(String message) {
            super(message);
        }
    }

    /**
     * 用户已存在该活动订单（不可重试，触发补偿）。
     */
    private static class DuplicateUserOrderException extends RuntimeException {
        DuplicateUserOrderException(String message) {
            super(message);
        }
    }

    /* 原实现（Redisson 用户锁 + 数据库条件扣减）保留：
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始！");
        }
        // 3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀已经结束！");
        }
        // 4.判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }

        return createVoucherOrder(voucherId);
    }



    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if(!isLock){
            // 获取锁失败，直接返回失败或者重试
            return Result.fail("不允许重复下单！");
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        } finally {
            // 释放锁
            redisLock.unlock();
        }

    }*/

    /* 原实现（SimpleRedisLock 分布式锁）保留：
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
        SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock(1200);
        // 判断
        if(!isLock){
            // 获取锁失败，直接返回失败或者重试
            return Result.fail("不允许重复下单！");
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        } finally {
            // 释放锁
            redisLock.unlock();
        }

    }*/

    /* 原实现（synchronized + 事务）保留：
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        synchronized (userId.toString().intern()) {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        }
    }*/
}
