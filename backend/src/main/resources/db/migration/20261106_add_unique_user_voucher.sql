-- 秒杀订单异步优化：一人一单数据库唯一约束
-- 执行前必须先运行重复数据预检；存在重复数据时须先与用户确认清理或保留策略，
-- 不得直接删除历史订单。

-- 预检 1：是否存在同一用户对同一活动的重复订单
-- 期望结果为空集
SELECT user_id, voucher_id, COUNT(*) AS cnt
FROM tb_voucher_order
GROUP BY user_id, voucher_id
HAVING cnt > 1;

-- 预检 2：确认索引尚未创建
SELECT INDEX_NAME
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'tb_voucher_order'
  AND INDEX_NAME = 'uk_user_voucher';

-- 建立唯一索引：一人一单的最终防线
ALTER TABLE tb_voucher_order
    ADD UNIQUE KEY uk_user_voucher (user_id, voucher_id);
