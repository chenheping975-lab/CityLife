package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 秒杀订单创建事件（消息契约见 specs/001-seckill-order-async/contracts/order-event.md）。
 *
 * <p>订单编号使用字符串，避免 JavaScript 数值精度丢失。</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillOrderMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 契约版本，当前为 1 */
    private Integer version;

    /** 服务端生成的订单编号（字符串精确值） */
    private String orderId;

    /** 下单用户标识（服务端身份） */
    private Long userId;

    /** 活动标识 */
    private Long voucherId;

    /** 受理时间，epoch 毫秒 */
    private Long acceptedAt;
}
