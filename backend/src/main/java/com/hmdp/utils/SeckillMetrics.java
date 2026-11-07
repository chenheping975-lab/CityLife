package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_METRICS_KEY;

/**
 * 秒杀链路指标记录。
 *
 * <p>通过 {@code HINCRBY seckill:metrics:{yyyyMMdd}} 记录：
 * accepted、created、failed、compensated、resent、rateLimited、duplicate、
 * reservationMissing（预占记录缺失/不完整，需人工核对）、
 * invalidMessage（订单事件本身不合法，已 ack 丢弃）。
 * 指标保留 30 天。</p>
 */
@Slf4j
@Component
public class SeckillMetrics {

    /** 指标保留天数 */
    private static final long METRICS_TTL_DAYS = 30L;

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 指标自增 1。
     *
     * @param metric 指标名，取值见类注释
     */
    public void increment(String metric) {
        increment(metric, 1L);
    }

    /**
     * 指标自增指定值。
     *
     * @param metric 指标名
     * @param delta  增量
     */
    public void increment(String metric, long delta) {
        if (metric == null || metric.isEmpty()) {
            return;
        }
        String key = SECKILL_METRICS_KEY + LocalDate.now().format(DATE_FORMATTER);
        try {
            stringRedisTemplate.opsForHash().increment(key, metric, delta);
            stringRedisTemplate.expire(key, METRICS_TTL_DAYS, TimeUnit.DAYS);
        } catch (Exception e) {
            // 指标失败不得影响主链路
            log.warn("秒杀指标写入失败，metric={}, delta={}", metric, delta, e);
        }
    }
}
