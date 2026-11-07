package com.hmdp.utils;

/**
 * 秒杀预占状态、用户可见状态与 Lua 返回码的映射。
 *
 * <p>集中定义，便于单元测试与前后端契约保持一致。</p>
 */
public final class SeckillOrderStatus {

    /** 预占状态：Lua 已预占，尚未确认 MQ 发送成功 */
    public static final String RESERVED = "RESERVED";
    /** 预占状态：订单事件已发送到 RocketMQ */
    public static final String SENT = "SENT";
    /** 预占状态：订单已落库（终态） */
    public static final String SUCCESS = "SUCCESS";
    /** 预占状态：创建最终失败，库存与资格已释放（终态） */
    public static final String COMPENSATED = "COMPENSATED";

    /** 用户可见状态：处理中 */
    public static final String USER_PROCESSING = "PROCESSING";
    /** 用户可见状态：已成功 */
    public static final String USER_SUCCESS = "SUCCESS";
    /** 用户可见状态：已失败 */
    public static final String USER_FAILED = "FAILED";

    /** 受理脚本返回码 */
    public static final int CODE_SUCCESS = 0;
    public static final int CODE_STOCK_EMPTY = 1;
    public static final int CODE_DUPLICATE = 2;
    public static final int CODE_NOT_BEGIN = 3;
    public static final int CODE_ENDED = 4;
    public static final int CODE_RATE_LIMITED = 5;
    public static final int CODE_UNAVAILABLE = 6;

    private SeckillOrderStatus() {
    }

    /**
     * 预占状态映射为用户可见状态。
     */
    public static String toUserStatus(String reservationStatus) {
        if (SUCCESS.equals(reservationStatus)) {
            return USER_SUCCESS;
        }
        if (COMPENSATED.equals(reservationStatus)) {
            return USER_FAILED;
        }
        // RESERVED / SENT / 记录缺失均视为处理中
        return USER_PROCESSING;
    }

    /**
     * 受理脚本返回码映射为中文提示。
     */
    public static String messageOf(int code) {
        switch (code) {
            case CODE_SUCCESS:
                return "抢购成功，处理中";
            case CODE_STOCK_EMPTY:
                return "库存不足";
            case CODE_DUPLICATE:
                return "请勿重复下单";
            case CODE_NOT_BEGIN:
                return "活动尚未开始";
            case CODE_ENDED:
                return "活动已结束";
            case CODE_RATE_LIMITED:
                return "请求过于频繁，请稍后再试";
            case CODE_UNAVAILABLE:
            default:
                return "活动不存在或已结束";
        }
    }

    /**
     * 是否为终态。
     */
    public static boolean isTerminal(String reservationStatus) {
        return SUCCESS.equals(reservationStatus) || COMPENSATED.equals(reservationStatus);
    }
}
