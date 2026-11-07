package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.SeckillOrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.common.message.MessageConst;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 秒杀订单事件生产者。
 *
 * <p>同步发送到 {@code seckill-order-topic} 的 {@code create} tag，消息 key 为订单编号。
 * 发送失败不抛出异常：预占记录保持 RESERVED，由对账任务重发，保证“已受理必有着落”。</p>
 */
@Slf4j
@Component
public class SeckillOrderProducer {

    /** 订单创建事件标签 */
    public static final String CREATE_TAG = "create";

    /** 默认发送超时（毫秒） */
    private static final long DEFAULT_SEND_TIMEOUT = 3000L;

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Value("${seckill.mq.topic:seckill-order-topic}")
    private String topic;

    @Value("${rocketmq.producer.send-message-timeout:3000}")
    private long sendTimeout;

    /**
     * 发送订单创建事件。
     *
     * @param message 订单创建事件
     * @return true 表示 Broker 已确认接收；false 表示发送失败，需要由对账任务重发
     */
    public boolean send(SeckillOrderMessage message) {
        if (message == null || message.getOrderId() == null) {
            log.error("秒杀订单事件为空，拒绝发送");
            return false;
        }
        long timeout = sendTimeout > 0 ? sendTimeout : DEFAULT_SEND_TIMEOUT;
        String destination = topic + ":" + CREATE_TAG;
        try {
            String payload = JSONUtil.toJsonStr(message);
            SendResult result = rocketMQTemplate.syncSend(
                    destination,
                    MessageBuilder.withPayload(payload)
                            .setHeader(MessageConst.PROPERTY_KEYS, message.getOrderId())
                            .build(),
                    timeout
            );
            boolean success = result != null && SendStatus.SEND_OK.equals(result.getSendStatus());
            if (success) {
                log.info("秒杀订单事件发送成功，orderId={}, userId={}, voucherId={}, msgId={}",
                        message.getOrderId(), message.getUserId(), message.getVoucherId(), result.getMsgId());
            } else {
                log.error("秒杀订单事件发送未确认，orderId={}, result={}", message.getOrderId(), result);
            }
            return success;
        } catch (Exception e) {
            log.error("秒杀订单事件发送异常，orderId={}, userId={}, voucherId={}",
                    message.getOrderId(), message.getUserId(), message.getVoucherId(), e);
            return false;
        }
    }
}
