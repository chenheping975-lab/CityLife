package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_RECONCILE_LOCK_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_RESERVATION_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_RESERVATION_PENDING_KEY;

/**
 * 秒杀预占记录读写。
 *
 * <p>预占状态严格限定为 {@link SeckillOrderStatus#RESERVED}、
 * {@link SeckillOrderStatus#SENT}、{@link SeckillOrderStatus#SUCCESS}、
 * {@link SeckillOrderStatus#COMPENSATED}。状态推进使用 Lua 比较后更新，
 * 避免重复消息或延迟确认把终态改回中间态。</p>
 */
@Component
public class SeckillReservationHelper {

    /** 单订单处理锁时长：30 秒，覆盖一次数据库事务与 Redis 更新 */
    private static final long ORDER_LOCK_SECONDS = 30L;

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    private static final DefaultRedisScript<Long> MARK_SENT_SCRIPT;
    private static final DefaultRedisScript<Long> MARK_SUCCESS_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setScriptText(
                "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end");
        UNLOCK_SCRIPT.setResultType(Long.class);

        // 仅 RESERVED -> SENT，避免把 SUCCESS/COMPENSATED 改回中间态
        MARK_SENT_SCRIPT = new DefaultRedisScript<>();
        MARK_SENT_SCRIPT.setScriptText(
                "local status = redis.call('HGET', KEYS[1], 'status') "
                        + "if status == 'RESERVED' then "
                        + "redis.call('HSET', KEYS[1], 'status', 'SENT', 'updatedAt', ARGV[1]) "
                        + "return 1 end return 0");
        MARK_SENT_SCRIPT.setResultType(Long.class);

        // 非 COMPENSATED 时置为 SUCCESS；已补偿的记录绝不回退；记录不存在时不得新建
        // 原实现：
        //   local status = redis.call('HGET', KEYS[1], 'status')
        //   if status == 'COMPENSATED' then return -1 end
        //   redis.call('HSET', KEYS[1], 'status', 'SUCCESS', 'updatedAt', ARGV[1])
        //   return 1
        // 修改原因：HGET 在键缺失时返回 false，会绕过 COMPENSATED 判断，被 HSET 新建出一个
        // 只有 status/updatedAt、没有 TTL 的空壳记录。它既掩盖了“订单已落库、预占记录已丢失”
        // 的不一致，又会让状态查询因缺少 userId 而误判为“订单不存在”，同时永久泄漏键。
        // 因此增加 EXISTS 守卫，返回 -3 表示记录不存在；HSET 也不会改变原记录的 TTL。
        MARK_SUCCESS_SCRIPT = new DefaultRedisScript<>();
        MARK_SUCCESS_SCRIPT.setScriptText(
                "if redis.call('EXISTS', KEYS[1]) == 0 then return -3 end "
                        + "local status = redis.call('HGET', KEYS[1], 'status') "
                        + "if status == 'COMPENSATED' then return -1 end "
                        + "redis.call('HSET', KEYS[1], 'status', 'SUCCESS', 'updatedAt', ARGV[1]) "
                        + "return 1");
        MARK_SUCCESS_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public String reservationKey(String orderId) {
        return SECKILL_RESERVATION_KEY + orderId;
    }

    /**
     * 读取预占记录，记录不存在返回 null。
     */
    public Map<Object, Object> get(String orderId) {
        Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries(reservationKey(orderId));
        return entries.isEmpty() ? null : entries;
    }

    /**
     * 读取预占状态，记录不存在返回 null。
     */
    public String getStatus(String orderId) {
        Object status = stringRedisTemplate.opsForHash().get(reservationKey(orderId), "status");
        return status == null ? null : status.toString();
    }

    /**
     * 读取预占记录中的用户标识。
     */
    public Long getUserId(String orderId) {
        return getLongField(orderId, "userId");
    }

    /**
     * 读取预占记录中的活动标识。
     */
    public Long getVoucherId(String orderId) {
        return getLongField(orderId, "voucherId");
    }

    /**
     * 读取受理时间（epoch 毫秒）。
     */
    public Long getAcceptedAt(String orderId) {
        return getLongField(orderId, "acceptedAt");
    }

    /**
     * 读取失败原因。
     */
    public String getFailReason(String orderId) {
        Object reason = stringRedisTemplate.opsForHash().get(reservationKey(orderId), "failReason");
        return reason == null ? null : reason.toString();
    }

    /**
     * 读取已发送次数。
     */
    public long getSendAttempts(String orderId) {
        Long value = getLongField(orderId, "sendAttempts");
        return value == null ? 0L : value;
    }

    /**
     * 发送次数加 1。
     */
    public long incrementSendAttempts(String orderId) {
        Long attempts = stringRedisTemplate.opsForHash()
                .increment(reservationKey(orderId), "sendAttempts", 1L);
        return attempts == null ? 1L : attempts;
    }

    /**
     * RESERVED -> SENT。返回 true 表示状态已推进。
     */
    public boolean markSent(String orderId, long nowMillis) {
        Long result = stringRedisTemplate.execute(MARK_SENT_SCRIPT,
                Collections.singletonList(reservationKey(orderId)), String.valueOf(nowMillis));
        return result != null && result == 1L;
    }

    /**
     * 置为 SUCCESS。
     *
     * @return 1 已置为 SUCCESS；-1 记录已是 COMPENSATED，调用方需要处理冲突；
     *         -3 记录不存在（脚本不会新建空壳记录）；-2 脚本返回空（异常兜底）
     */
    public long markSuccess(String orderId, long nowMillis) {
        Long result = stringRedisTemplate.execute(MARK_SUCCESS_SCRIPT,
                Collections.singletonList(reservationKey(orderId)), String.valueOf(nowMillis));
        return result == null ? -2L : result;
    }

    /**
     * 从待处理集合移除。
     */
    public void removePending(String orderId) {
        stringRedisTemplate.opsForZSet().remove(SECKILL_RESERVATION_PENDING_KEY, orderId);
    }

    /**
     * 按受理时间扫描待处理订单。
     */
    public Set<String> pendingOrders(long maxScore, int limit) {
        Set<String> orders = stringRedisTemplate.opsForZSet()
                .rangeByScore(SECKILL_RESERVATION_PENDING_KEY, 0, maxScore, 0, limit);
        return orders == null ? Collections.emptySet() : orders;
    }

    /**
     * 获取单订单处理锁。
     *
     * @return 锁 token；获取失败返回 null
     */
    public String tryLockOrder(String orderId) {
        String token = UUID.randomUUID().toString();
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(SECKILL_RECONCILE_LOCK_KEY + orderId, token, ORDER_LOCK_SECONDS, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success) ? token : null;
    }

    /**
     * 释放单订单处理锁。
     */
    public void unlockOrder(String orderId, String token) {
        if (token == null) {
            return;
        }
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(SECKILL_RECONCILE_LOCK_KEY + orderId), token);
    }

    private Long getLongField(String orderId, String field) {
        Object value = stringRedisTemplate.opsForHash().get(reservationKey(orderId), field);
        if (value == null) {
            return null;
        }
        try {
            return Long.valueOf(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
