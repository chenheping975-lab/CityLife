package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 秒杀受理结果。
 *
 * <p>{@code status} 固定为 {@code PROCESSING}，表示请求已受理并完成预占，
 * 不代表订单已落库；用户需通过订单编号查询最终状态。</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillOrderResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 订单编号，字符串精确值 */
    private String orderId;

    /** 受理状态，固定 PROCESSING */
    private String status;
}
