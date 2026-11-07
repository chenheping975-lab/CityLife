package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    /** 秒杀活动元数据：seckill:activity:{voucherId}，Hash */
    public static final String SECKILL_ACTIVITY_KEY = "seckill:activity:";
    /** 固定窗口限流：seckill:rate:{voucherId}:{userId} */
    public static final String SECKILL_RATE_KEY = "seckill:rate:";
    /** 已下单用户集合：seckill:order:{voucherId}，Set，member=userId */
    public static final String SECKILL_ORDER_KEY = "seckill:order:";
    /** 预占记录：seckill:order:reservation:{orderId}，Hash */
    public static final String SECKILL_RESERVATION_KEY = "seckill:order:reservation:";
    /** 待处理预占集合：ZSet，member=orderId，score=acceptedAt */
    public static final String SECKILL_RESERVATION_PENDING_KEY = "seckill:order:reservation:pending";
    /** 秒杀指标：seckill:metrics:{yyyyMMdd}，Hash */
    public static final String SECKILL_METRICS_KEY = "seckill:metrics:";
    /** 对账任务单订单短锁 */
    public static final String SECKILL_RECONCILE_LOCK_KEY = "seckill:order:reconcile:lock:";
    /** 活动元数据回填互斥锁 */
    public static final String SECKILL_ACTIVITY_LOCK_KEY = "lock:seckill:activity:";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
}
