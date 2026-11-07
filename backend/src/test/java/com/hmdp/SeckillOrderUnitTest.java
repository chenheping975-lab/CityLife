package com.hmdp;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.dto.SeckillOrderStatusDTO;
import com.hmdp.utils.SeckillOrderStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 秒杀订单纯单元测试：不依赖 MySQL / Redis / RocketMQ。
 */
class SeckillOrderUnitTest {

    private static final String LARGE_ORDER_ID = "1834567890123456789";

    @Test
    @DisplayName("订单编号在 JSON 序列化与反序列化过程中保持字符串精确值")
    void orderIdKeepsStringPrecision() {
        SeckillOrderMessage message = new SeckillOrderMessage(
                1, LARGE_ORDER_ID, 1010L, 10L, 1791234567890L);

        String json = JSONUtil.toJsonStr(message);

        assertTrue(json.contains("\"orderId\":\"" + LARGE_ORDER_ID + "\""),
                "订单编号必须以字符串形式序列化，实际 JSON：" + json);

        JSONObject parsed = JSONUtil.parseObj(json);
        assertTrue(parsed.get("orderId") instanceof CharSequence,
                "反序列化后订单编号必须仍是字符串");
        assertEquals(LARGE_ORDER_ID, parsed.getStr("orderId"));

        SeckillOrderMessage restored = JSONUtil.toBean(json, SeckillOrderMessage.class);
        assertEquals(LARGE_ORDER_ID, restored.getOrderId());
        assertEquals(1010L, restored.getUserId());
        assertEquals(10L, restored.getVoucherId());
        assertEquals(1791234567890L, restored.getAcceptedAt());
        assertEquals(1, restored.getVersion());
    }

    @Test
    @DisplayName("预占状态映射为用户可见状态")
    void reservationStatusMapsToUserStatus() {
        assertEquals(SeckillOrderStatus.USER_PROCESSING,
                SeckillOrderStatus.toUserStatus(SeckillOrderStatus.RESERVED));
        assertEquals(SeckillOrderStatus.USER_PROCESSING,
                SeckillOrderStatus.toUserStatus(SeckillOrderStatus.SENT));
        assertEquals(SeckillOrderStatus.USER_SUCCESS,
                SeckillOrderStatus.toUserStatus(SeckillOrderStatus.SUCCESS));
        assertEquals(SeckillOrderStatus.USER_FAILED,
                SeckillOrderStatus.toUserStatus(SeckillOrderStatus.COMPENSATED));
        // 记录缺失按处理中展示，不泄露内部状态
        assertEquals(SeckillOrderStatus.USER_PROCESSING,
                SeckillOrderStatus.toUserStatus(null));
    }

    @Test
    @DisplayName("Lua 返回码映射为约定的中文提示")
    void luaCodeMapsToMessage() {
        assertEquals("抢购成功，处理中", SeckillOrderStatus.messageOf(0));
        assertEquals("库存不足", SeckillOrderStatus.messageOf(1));
        assertEquals("请勿重复下单", SeckillOrderStatus.messageOf(2));
        assertEquals("活动尚未开始", SeckillOrderStatus.messageOf(3));
        assertEquals("活动已结束", SeckillOrderStatus.messageOf(4));
        assertEquals("请求过于频繁，请稍后再试", SeckillOrderStatus.messageOf(5));
        assertEquals("活动不存在或已结束", SeckillOrderStatus.messageOf(6));
        assertEquals("活动不存在或已结束", SeckillOrderStatus.messageOf(99));
    }

    @Test
    @DisplayName("终态判断")
    void terminalStatus() {
        assertFalse(SeckillOrderStatus.isTerminal(SeckillOrderStatus.RESERVED));
        assertFalse(SeckillOrderStatus.isTerminal(SeckillOrderStatus.SENT));
        assertTrue(SeckillOrderStatus.isTerminal(SeckillOrderStatus.SUCCESS));
        assertTrue(SeckillOrderStatus.isTerminal(SeckillOrderStatus.COMPENSATED));
        assertFalse(SeckillOrderStatus.isTerminal(null));
    }

    @Test
    @DisplayName("订单编号边界：JS 安全整数上界与超界编号均按字符串精确往返")
    void orderIdBoundaryKeepsStringPrecision() {
        String[] ids = {"9007199254740991", "9007199254740993", "9223372036854775807"};
        for (String id : ids) {
            String json = JSONUtil.toJsonStr(new SeckillOrderMessage(1, id, 1L, 2L, 3L));
            assertTrue(json.contains("\"" + id + "\""), "订单编号必须以字符串序列化，实际：" + json);
            assertEquals(id, JSONUtil.toBean(json, SeckillOrderMessage.class).getOrderId());
        }
        // 证明数值化会丢失精度：超过 2^53 的编号不能被 Number 精确表示，系统全程禁止该转换
        String beyondSafe = "9007199254740993";
        assertNotEquals(beyondSafe, String.valueOf((long) Double.parseDouble(beyondSafe)));
        assertEquals("9007199254740991", String.valueOf((long) Double.parseDouble("9007199254740991")),
                "安全整数上界以内可以精确表示为 double");
    }

    @Test
    @DisplayName("状态查询 DTO 序列化时必须保留 null 的 reason 字段")
    void statusDtoKeepsNullReason() throws Exception {
        SeckillOrderStatusDTO dto = new SeckillOrderStatusDTO(
                "657149830446972929", SeckillOrderStatus.USER_PROCESSING, null, 1793971018221L);

        String json = new ObjectMapper().writeValueAsString(dto);

        assertTrue(json.contains("\"reason\":null"), "reason 为 null 时也必须输出字段，实际：" + json);
        assertTrue(json.contains("\"orderId\":\"657149830446972929\""), "订单编号必须是 JSON 字符串");
        assertTrue(json.contains("\"status\":\"PROCESSING\""));
    }
}
