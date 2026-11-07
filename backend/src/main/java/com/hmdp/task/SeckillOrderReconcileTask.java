package com.hmdp.task;

import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.mq.SeckillOrderProducer;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.SeckillMetrics;
import com.hmdp.utils.SeckillOrderStatus;
import com.hmdp.utils.SeckillReservationHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;

/**
 * 秒杀预占对账任务。
 *
 * <p>扫描 {@code seckill:order:reservation:pending}：</p>
 * <ul>
 *     <li>RESERVED 超时未确认发送：重发订单事件；超过最大次数则补偿；</li>
 *     <li>SENT 超时且数据库无订单：重发或补偿；</li>
 *     <li>数据库已有订单：修复预占状态为 SUCCESS 并移出待处理；</li>
 *     <li>SUCCESS / COMPENSATED：直接移出待处理。</li>
 * </ul>
 *
 * <p>每个订单使用短 TTL 锁，降低多实例重复处理；与消费者、补偿共用同一把锁，
 * 避免“补偿释放库存”与“订单落库”并发交错。</p>
 */
@Slf4j
@Component
public class SeckillOrderReconcileTask {

    @Resource
    private SeckillReservationHelper reservationHelper;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private SeckillOrderProducer seckillOrderProducer;

    @Resource
    private SeckillMetrics seckillMetrics;

    /** RESERVED 超过该时长仍未确认发送成功则重发 */
    @Value("${seckill.reconcile.resend-threshold-ms:5000}")
    private long resendThresholdMs;

    /** 单个预占最大发送次数 */
    @Value("${seckill.reconcile.max-send-attempts:5}")
    private long maxSendAttempts;

    /** SENT 超过该时长数据库仍无订单则重发或补偿 */
    @Value("${seckill.reconcile.compensate-threshold-ms:60000}")
    private long compensateThresholdMs;

    /** 单次扫描条数 */
    @Value("${seckill.reconcile.batch-size:100}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${seckill.reconcile.interval-ms:10000}")
    public void reconcile() {
        long now = System.currentTimeMillis();
        // 只扫描已超过重发阈值的预占，避免与正常受理/消费竞争
        Set<String> orderIds = reservationHelper.pendingOrders(now - Math.min(resendThresholdMs, compensateThresholdMs), batchSize);
        if (orderIds == null || orderIds.isEmpty()) {
            return;
        }
        log.debug("秒杀预占对账开始，待处理数量={}", orderIds.size());
        for (String orderId : orderIds) {
            String lockToken = reservationHelper.tryLockOrder(orderId);
            if (lockToken == null) {
                // 消费者或另一实例正在处理
                continue;
            }
            try {
                ReconcileAction action = evaluate(orderId, now);
                if (action == ReconcileAction.MARK_SUCCESS) {
                    long markResult = reservationHelper.markSuccess(orderId, now);
                    // 原实现：忽略返回值，直接记录“修复成功”。
                    // 修改原因：MARK_SUCCESS 脚本在记录缺失时改为返回 -3（不再新建空壳记录），
                    // 若仍按成功记录日志，会出现“日志报成功、实际未修复”的静默不一致。
                    if (markResult == -3L) {
                        log.error("订单已存在但预占记录缺失，无法修复预占状态，需人工核对，orderId={}", orderId);
                        seckillMetrics.increment("reservationMissing");
                    } else {
                        log.info("对账修复预占状态为 SUCCESS，orderId={}", orderId);
                    }
                    reservationHelper.removePending(orderId);
                } else if (action == ReconcileAction.RESEND) {
                    resend(orderId, now);
                } else if (action == ReconcileAction.COMPENSATE) {
                    // 释放本任务的锁后再走补偿；补偿方法会重新获取同一把锁
                    reservationHelper.unlockOrder(orderId, lockToken);
                    lockToken = null;
                    voucherOrderService.compensateOrder(orderId, "订单事件重试次数耗尽");
                }
            } catch (Exception e) {
                log.error("秒杀预占对账处理异常，orderId={}", orderId, e);
            } finally {
                if (lockToken != null) {
                    reservationHelper.unlockOrder(orderId, lockToken);
                }
            }
        }
    }

    /**
     * 判断单个预占需要执行的动作。调用方必须已持有订单锁。
     */
    private ReconcileAction evaluate(String orderId, long now) {
        String status = reservationHelper.getStatus(orderId);
        if (status == null) {
            // 预占记录已过期：清理待处理集合
            reservationHelper.removePending(orderId);
            log.warn("预占记录已过期，移出待处理集合，orderId={}", orderId);
            return ReconcileAction.NONE;
        }

        Long orderIdValue = parseOrderId(orderId);
        boolean orderExists = orderIdValue != null && voucherOrderService.getById(orderIdValue) != null;
        if (orderExists) {
            return ReconcileAction.MARK_SUCCESS;
        }

        if (SeckillOrderStatus.SUCCESS.equals(status)) {
            reservationHelper.removePending(orderId);
            return ReconcileAction.NONE;
        }
        if (SeckillOrderStatus.COMPENSATED.equals(status)) {
            reservationHelper.removePending(orderId);
            return ReconcileAction.NONE;
        }

        Long acceptedAt = reservationHelper.getAcceptedAt(orderId);
        if (acceptedAt == null) {
            log.warn("预占记录缺少受理时间，移出待处理集合，orderId={}", orderId);
            reservationHelper.removePending(orderId);
            return ReconcileAction.NONE;
        }

        long age = now - acceptedAt;
        long threshold = SeckillOrderStatus.RESERVED.equals(status) ? resendThresholdMs : compensateThresholdMs;
        if (age < threshold) {
            return ReconcileAction.NONE;
        }

        // 达到重发条件：累计发送次数，超过上限则补偿
        long attempts = reservationHelper.incrementSendAttempts(orderId);
        if (attempts > maxSendAttempts) {
            log.error("预占发送次数耗尽，准备补偿，orderId={}, attempts={}", orderId, attempts);
            return ReconcileAction.COMPENSATE;
        }
        return ReconcileAction.RESEND;
    }

    /**
     * 重发订单事件。调用方必须已持有订单锁。
     */
    private void resend(String orderId, long now) {
        Long userId = reservationHelper.getUserId(orderId);
        Long voucherId = reservationHelper.getVoucherId(orderId);
        Long acceptedAt = reservationHelper.getAcceptedAt(orderId);
        if (userId == null || voucherId == null) {
            log.error("预占记录缺少 userId/voucherId，无法重发，orderId={}", orderId);
            reservationHelper.removePending(orderId);
            return;
        }
        SeckillOrderMessage message = new SeckillOrderMessage(
                1, orderId, userId, voucherId,
                acceptedAt == null ? now : acceptedAt);
        boolean sent = seckillOrderProducer.send(message);
        if (sent) {
            // RESERVED -> SENT；已经是 SENT 时脚本不改变状态
            reservationHelper.markSent(orderId, now);
            seckillMetrics.increment("resent");
            log.info("对账重发订单事件成功，orderId={}, userId={}, voucherId={}", orderId, userId, voucherId);
        } else {
            log.warn("对账重发订单事件失败，等待下一轮，orderId={}", orderId);
        }
    }

    private Long parseOrderId(String orderId) {
        if (orderId == null || orderId.isEmpty() || orderId.length() > 19) {
            return null;
        }
        for (int i = 0; i < orderId.length(); i++) {
            if (!Character.isDigit(orderId.charAt(i))) {
                return null;
            }
        }
        try {
            return Long.valueOf(orderId);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 对账动作。
     */
    private enum ReconcileAction {
        NONE,
        MARK_SUCCESS,
        RESEND,
        COMPENSATE
    }
}
