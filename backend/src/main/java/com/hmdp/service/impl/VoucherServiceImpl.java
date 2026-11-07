package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.SeckillActivityCache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private SeckillActivityCache seckillActivityCache;

    @Override
    public Result queryVoucherOfShop(Long shopId) {
        // 查询优惠券信息
        List<Voucher> vouchers = getBaseMapper().queryVoucherOfShop(shopId);
        // 返回结果
        return Result.ok(vouchers);
    }

    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        // 保存优惠券
        save(voucher);
        // 保存秒杀信息
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        seckillVoucherService.save(seckillVoucher);

        // 原实现：无条件覆盖 Redis 库存。
        // 修改原因：秒杀订单异步化后 Redis 库存是运行期扣减的唯一入口，
        // 无条件覆盖会在重复新增或重启补数据时把已扣减的库存冲回初始值，造成超卖。
        // stringRedisTemplate.opsForValue().set(SECKILL_STOCK_KEY + voucher.getId(), voucher.getStock().toString());

        // 新实现：写入活动元数据，并仅在库存 key 缺失时初始化，避免覆盖运行期库存。
        LocalDateTime beginTime = voucher.getBeginTime();
        LocalDateTime endTime = voucher.getEndTime();
        long endTimeMillis = endTime == null ? System.currentTimeMillis()
                : endTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long beginTimeMillis = beginTime == null ? System.currentTimeMillis()
                : beginTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        int status = voucher.getStatus() == null ? 1 : voucher.getStatus();
        int stock = voucher.getStock() == null ? 0 : voucher.getStock();
        // 原实现：以下两处 Redis 写入直接在本方法的数据库事务内执行。
        // 修改原因：MySQL 与 Redis 无法组成原子操作。若事务在 Redis 写入之后回滚（如
        // tb_seckill_voucher 插入失败、提交阶段异常），会残留“数据库不存在、Redis 仍可抢购”的
        // 幽灵活动：用户能受理成功并扣减 Redis 库存，但消费者落库时找不到对应优惠券，
        // 只能逐笔补偿，用户看到 FAILED；而 initialStock 已在首次写入时锁定，不会自愈。
        // 因此改为事务提交后再初始化 Redis，提交失败则不写入。
        Long voucherId = voucher.getId();
        runAfterCommit(voucherId, () -> initRedisActivity(voucherId, beginTimeMillis, endTimeMillis, status, stock));
    }

    /**
     * 在当前事务提交后执行 Redis 初始化；不存在活动事务时立即执行。
     *
     * @param voucherId 仅用于失败日志定位
     * @param action    事务提交后要执行的 Redis 写入
     */
    private void runAfterCommit(Long voucherId, Runnable action) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            action.run();
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    action.run();
                } catch (RuntimeException e) {
                    // 数据库已提交，Redis 初始化失败不应反向影响已成功的新增操作：
                    // 记录告警，等待用户首次抢购时经 ensureActivity 从数据库回填。
                    log.error("秒杀活动 Redis 初始化失败，将由首次抢购时的数据库回填补偿，voucherId={}",
                            voucherId, e);
                }
            }
        });
    }

    private void initRedisActivity(Long voucherId, long beginTimeMillis, long endTimeMillis,
                                   int status, int stock) {
        seckillActivityCache.save(voucherId, beginTimeMillis, endTimeMillis, status, stock);
        stringRedisTemplate.opsForValue().setIfAbsent(
                SECKILL_STOCK_KEY + voucherId,
                String.valueOf(stock),
                seckillActivityCache.ttlSeconds(endTimeMillis),
                TimeUnit.SECONDS);
    }
}
