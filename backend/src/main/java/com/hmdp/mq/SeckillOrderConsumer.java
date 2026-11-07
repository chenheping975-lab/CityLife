package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 秒杀订单创建消费者。
 *
 * <p>以订单编号为幂等键创建订单；处理失败必须抛出异常交由 RocketMQ 重试，
 * 超过最大重试次数后进入死信 Topic，由 {@link SeckillOrderDLQConsumer} 执行补偿。</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "seckill.mq", name = "consumer-enabled", havingValue = "true", matchIfMissing = true)
@RocketMQMessageListener(
        topic = "${seckill.mq.topic:seckill-order-topic}",
        selectorExpression = SeckillOrderProducer.CREATE_TAG,
        consumerGroup = "${seckill.mq.consumer-group:seckill-order-consumer-group}"
)
public class SeckillOrderConsumer implements RocketMQListener<String> {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Override
    public void onMessage(String body) {
        log.debug("收到秒杀订单事件：{}", body);
        SeckillOrderMessage message;
        try {
            message = JSONUtil.toBean(body, SeckillOrderMessage.class);
        } catch (Exception e) {
            // 解析失败必须抛出，交给 RocketMQ 重试/死信，不得静默丢弃
            log.error("秒杀订单事件解析失败：{}", body, e);
            throw new IllegalArgumentException("秒杀订单事件解析失败", e);
        }
        if (message == null || message.getOrderId() == null) {
            throw new IllegalArgumentException("秒杀订单事件缺少订单编号");
        }
        voucherOrderService.createVoucherOrder(message);
    }
}
