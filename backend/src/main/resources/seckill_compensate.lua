-- 秒杀预占幂等补偿脚本：释放库存与购买资格，状态置为 COMPENSATED。
-- ARGV[1] orderId
-- ARGV[2] voucherId（预占记录缺失该字段时使用）
-- ARGV[3] userId（预占记录缺失该字段时使用）
-- ARGV[4] reason
-- ARGV[5] nowMillis
-- 返回码：0 本次完成补偿；1 已处于终态，无需补偿；2 预占记录不存在

local orderId = ARGV[1]
local voucherId = ARGV[2]
local userId = ARGV[3]
local reason = ARGV[4]
local nowMillis = ARGV[5]

local reservationKey = 'seckill:order:reservation:' .. orderId
local pendingKey = 'seckill:order:reservation:pending'

-- 1. 预占记录不存在：已过期或参数错误
if redis.call('EXISTS', reservationKey) == 0 then
    return 2
end

-- 2. 终态幂等：SUCCESS / COMPENSATED 不再补偿
local status = redis.call('HGET', reservationKey, 'status')
if status == 'SUCCESS' or status == 'COMPENSATED' then
    return 1
end

-- 3. 以预占记录中的业务字段为准，避免调用方传参不一致
local storedVoucherId = redis.call('HGET', reservationKey, 'voucherId')
if storedVoucherId ~= nil then
    voucherId = storedVoucherId
end
local storedUserId = redis.call('HGET', reservationKey, 'userId')
if storedUserId ~= nil then
    userId = storedUserId
end

-- 4. 仅当当前库存小于初始库存时回补 1，库存不得超过初始值
local activityKey = 'seckill:activity:' .. voucherId
local initialStock = tonumber(redis.call('HGET', activityKey, 'initialStock'))
local stockKey = 'seckill:stock:' .. voucherId
local currentStock = tonumber(redis.call('GET', stockKey))
if initialStock ~= nil and currentStock ~= nil and currentStock < initialStock then
    redis.call('INCR', stockKey)
end

-- 5. 释放购买资格
redis.call('SREM', 'seckill:order:' .. voucherId, userId)

-- 6. 标记为已补偿并移出待处理集合
redis.call('HSET', reservationKey,
        'status', 'COMPENSATED',
        'failReason', reason,
        'updatedAt', nowMillis)
redis.call('ZREM', pendingKey, orderId)

return 0
