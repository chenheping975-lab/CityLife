package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.SeckillMetrics;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 秒杀订单死信消费者。
 *
 * <p>消息重试耗尽后进入 {@code %DLQ%seckill-order-consumer-group}，
 * 这里触发幂等补偿：释放库存与购买资格，并把请求标记为最终失败。</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "seckill.mq", name = "consumer-enabled", havingValue = "true", matchIfMissing = true)
@RocketMQMessageListener(
        topic = "${seckill.mq.dlq-topic:%DLQ%seckill-order-consumer-group}",
        consumerGroup = "${seckill.mq.dlq-consumer-group:seckill-order-dlq-consumer-group}"
)
public class SeckillOrderDLQConsumer implements RocketMQListener<String> {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private SeckillMetrics seckillMetrics;

    @Override
    public void onMessage(String body) {
        log.error("收到秒杀订单死信：{}", body);
        SeckillOrderMessage message;
        try {
            message = JSONUtil.toBean(body, SeckillOrderMessage.class);
        } catch (Exception e) {
            log.error("死信消息解析失败，无法补偿：{}", body, e);
            return;
        }
        if (message == null || message.getOrderId() == null) {
            log.error("死信消息缺少订单编号，无法补偿：{}", body);
            return;
        }
        seckillMetrics.increment("failed");
        voucherOrderService.compensateOrder(message.getOrderId(), "订单事件重试次数耗尽");
    }
}
