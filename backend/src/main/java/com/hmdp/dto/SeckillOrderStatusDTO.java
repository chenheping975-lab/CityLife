package com.hmdp.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 秒杀订单状态查询结果。
 *
 * <p>状态取值：PROCESSING（处理中）、SUCCESS（已成功）、FAILED（已失败）。</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.ALWAYS)
public class SeckillOrderStatusDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 订单编号，字符串精确值 */
    private String orderId;

    /** 用户可见状态 */
    private String status;

    /** 失败原因，仅 FAILED 时有值 */
    private String reason;

    /** 受理时间，epoch 毫秒 */
    private Long acceptedAt;
}
