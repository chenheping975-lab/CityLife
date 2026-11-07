package com.hmdp.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 定时任务配置。
 *
 * <p>开启 {@code @Scheduled} 支持，供秒杀订单预占对账任务使用。</p>
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}
