package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_ACTIVITY_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ACTIVITY_LOCK_KEY;

/**
 * 秒杀活动元数据缓存。
 *
 * <p>{@code seckill:activity:{voucherId}} 为 Hash，字段：
 * beginTime、endTime（epoch 毫秒）、status（1 上架 / 2 下架 / 3 过期）、initialStock、
 * refreshedAt（最近一次从数据库刷新的时间，epoch 毫秒）。</p>
 *
 * <p>refreshedAt 用于按周期从数据库刷新可变的 status/beginTime/endTime，使下架、改时间最多在一个
 * 刷新周期内生效；initialStock 只在首次写入时取数据库值，之后刷新保持不变，避免补偿脚本的
 * “库存不得超过初始值”判断被数据库当前库存覆盖。</p>
 *
 * <p>本类只负责 Redis 读写与回填互斥；数据库回填由 Service 层完成，避免工具类直接访问 Mapper。</p>
 */
@Component
public class SeckillActivityCache {

    /** 活动结束后的保留时长：7 天 */
    private static final long RETAIN_AFTER_END_SECONDS = 7L * 24 * 3600;
    /** 元数据最短保留时长：30 天 */
    private static final long MIN_TTL_SECONDS = 30L * 24 * 3600;
    /** 回填互斥锁时长：3 秒 */
    private static final long LOAD_LOCK_SECONDS = 3L;

    /** 仅当锁值匹配时才删除，避免误删其他线程的锁 */
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setScriptText(
                "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end");
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public String activityKey(Long voucherId) {
        return SECKILL_ACTIVITY_KEY + voucherId;
    }

    /**
     * 活动元数据是否存在。
     */
    public boolean exists(Long voucherId) {
        return Boolean.TRUE.equals(stringRedisTemplate.hasKey(activityKey(voucherId)));
    }

    /**
     * 判断活动元数据是否仍在刷新周期内。
     *
     * <p>一次 HMGET 同时读取 refreshedAt 与 status：记录缺失、字段缺失或已过期都返回 false，
     * 由调用方触发数据库回刷。刷新间隔越短，下架/改时间生效越快，数据库压力越大。</p>
     *
     * @param voucherId      活动标识
     * @param intervalMillis 刷新间隔（毫秒）
     * @return true 表示无需从数据库刷新
     */
    public boolean isFresh(Long voucherId, long intervalMillis) {
        List<Object> values = stringRedisTemplate.opsForHash()
                .multiGet(activityKey(voucherId), Arrays.asList("refreshedAt", "status"));
        if (values == null || values.size() < 2 || values.get(0) == null || values.get(1) == null) {
            return false;
        }
        try {
            long refreshedAt = Long.parseLong(values.get(0).toString());
            return System.currentTimeMillis() - refreshedAt < intervalMillis;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 尝试获取回填互斥锁。
     *
     * @return 锁 token；获取失败返回 null
     */
    public String tryLock(Long voucherId) {
        String token = UUID.randomUUID().toString();
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(SECKILL_ACTIVITY_LOCK_KEY + voucherId, token, LOAD_LOCK_SECONDS, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success) ? token : null;
    }

    /**
     * 释放回填互斥锁。
     */
    public void unlock(Long voucherId, String token) {
        if (token == null) {
            return;
        }
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(SECKILL_ACTIVITY_LOCK_KEY + voucherId), token);
    }

    /**
     * 写入活动元数据。
     *
     * @param voucherId    活动标识
     * @param beginTime    开始时间，epoch 毫秒
     * @param endTime      结束时间，epoch 毫秒
     * @param status       1 上架 / 2 下架 / 3 过期
     * @param initialStock 初始库存（仅在首次写入时生效）
     */
    public void save(Long voucherId, long beginTime, long endTime, int status, int initialStock) {
        String key = activityKey(voucherId);
        Map<String, String> metadata = new HashMap<>(6);
        metadata.put("beginTime", String.valueOf(beginTime));
        metadata.put("endTime", String.valueOf(endTime));
        metadata.put("status", String.valueOf(status));
        metadata.put("refreshedAt", String.valueOf(System.currentTimeMillis()));
        // 原实现：每次 save 都用传入值覆盖 initialStock。
        // 修改原因：活动元数据会周期性从数据库刷新，而数据库中的 stock 是“当前库存”。
        // 若刷新时覆盖 initialStock，补偿脚本“库存不得超过初始值”的判断会失效，导致回补被跳过。
        // 因此 initialStock 只在首次写入时落库，后续刷新保持不变。
        Object existingInitialStock = stringRedisTemplate.opsForHash().get(key, "initialStock");
        if (existingInitialStock == null) {
            metadata.put("initialStock", String.valueOf(initialStock));
        }
        stringRedisTemplate.opsForHash().putAll(key, metadata);
        stringRedisTemplate.expire(key, ttlSeconds(endTime), TimeUnit.SECONDS);
    }

    /**
     * 读取活动结束时间，用于计算预占记录 TTL。
     *
     * @return epoch 毫秒；元数据缺失返回 null
     */
    public Long getEndTime(Long voucherId) {
        Object value = stringRedisTemplate.opsForHash().get(activityKey(voucherId), "endTime");
        if (value == null) {
            return null;
        }
        try {
            return Long.valueOf(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * TTL：活动结束后 7 天，且不少于 30 天。
     */
    public long ttlSeconds(long endTimeMillis) {
        long remainingSeconds = Math.max(0L, (endTimeMillis - System.currentTimeMillis()) / 1000L);
        return Math.max(MIN_TTL_SECONDS, remainingSeconds + RETAIN_AFTER_END_SECONDS);
    }
}
