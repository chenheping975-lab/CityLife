package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    /**
     * 消费秒杀订单创建事件，幂等创建订单。
     *
     * @param message 订单创建事件
     */
    void createVoucherOrder(SeckillOrderMessage message);

    /**
     * 查询订单/请求状态，仅允许查询本人订单。
     *
     * @param orderId 订单编号（字符串精确值）
     * @return 状态查询结果
     */
    Result queryOrderStatus(String orderId);

    /**
     * 幂等补偿：释放库存与购买资格，并把预占状态置为 COMPENSATED。
     *
     * @param orderId 订单编号
     * @param reason  失败原因
     */
    void compensateOrder(String orderId, String reason);
}
