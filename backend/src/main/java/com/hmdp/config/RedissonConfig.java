package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient(RedisProperties redisProperties){
        // Redisson 与 StringRedisTemplate 共用配置，避免凭据硬编码或数据库不一致。
        Config config = new Config();
        String scheme = redisProperties.isSsl() ? "rediss://" : "redis://";
        SingleServerConfig server = config.useSingleServer()
                .setAddress(scheme + redisProperties.getHost() + ":" + redisProperties.getPort())
                .setDatabase(redisProperties.getDatabase());
        if (redisProperties.getPassword() != null && !redisProperties.getPassword().isEmpty()) {
            server.setPassword(redisProperties.getPassword());
        }
        return Redisson.create(config);
    }
}
