package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    /**
     * 查询订单/请求状态。
     *
     * <p>路径参数按字符串接收，避免 JavaScript/数值转换造成订单编号精度丢失；
     * 订单编号合法性由 Service 统一校验。</p>
     */
    @GetMapping("{orderId}")
    public Result queryOrderStatus(@PathVariable("orderId") String orderId) {
        return voucherOrderService.queryOrderStatus(orderId);
    }
}
