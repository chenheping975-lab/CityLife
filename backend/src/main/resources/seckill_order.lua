-- 秒杀受理脚本：活动校验、限流、库存与一人一单判断、预占，全部原子完成。
-- ARGV[1] voucherId
-- ARGV[2] userId
-- ARGV[3] orderId
-- ARGV[4] serverTimeMillis（服务端时间，客户端不可伪造）
-- ARGV[5] rateLimit（每用户每活动每秒允许的请求次数）
-- 返回码：0 成功；1 库存不足；2 重复下单；3 未开始；4 已结束；5 限流；6 活动不存在/下架

local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
local serverTime = tonumber(ARGV[4])
local rateLimit = tonumber(ARGV[5])

local activityKey = 'seckill:activity:' .. voucherId
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
local rateKey = 'seckill:rate:' .. voucherId .. ':' .. userId
local reservationKey = 'seckill:order:reservation:' .. orderId
local pendingKey = 'seckill:order:reservation:pending'

-- 1. 活动元数据必须存在且为上架状态
if redis.call('EXISTS', activityKey) == 0 then
    return 6
end
local status = tonumber(redis.call('HGET', activityKey, 'status'))
local beginTime = tonumber(redis.call('HGET', activityKey, 'beginTime'))
local endTime = tonumber(redis.call('HGET', activityKey, 'endTime'))
if status ~= 1 or beginTime == nil or endTime == nil then
    return 6
end

-- 2. 活动时间只按服务端时间判断
if serverTime < beginTime then
    return 3
end
if serverTime > endTime then
    return 4
end

-- 3. 固定 1 秒窗口限流；被限流的请求不占用库存
local current = redis.call('INCR', rateKey)
if current == 1 then
    redis.call('PEXPIRE', rateKey, 1000)
end
if current > rateLimit then
    return 5
end

-- 4. 库存判断
local stock = tonumber(redis.call('GET', stockKey))
if stock == nil or stock <= 0 then
    return 1
end

-- 5. 一人一单判断
if redis.call('SISMEMBER', orderKey, userId) == 1 then
    return 2
end

-- 6. 原子预占：扣库存、记录购买资格、写入预占记录与待处理集合
redis.call('DECR', stockKey)
redis.call('SADD', orderKey, userId)
redis.call('HSET', reservationKey,
        'userId', userId,
        'voucherId', voucherId,
        'status', 'RESERVED',
        'acceptedAt', serverTime,
        'updatedAt', serverTime,
        'sendAttempts', 0)
redis.call('ZADD', pendingKey, serverTime, orderId)

-- 7. 预占记录与待处理集合的 TTL：活动结束后 7 天，且不少于 30 天
local ttl = math.max(30 * 24 * 3600, math.ceil((endTime - serverTime) / 1000) + 7 * 24 * 3600)
redis.call('EXPIRE', reservationKey, ttl)
redis.call('EXPIRE', pendingKey, ttl)
redis.call('EXPIRE', stockKey, ttl)
redis.call('EXPIRE', orderKey, ttl)

return 0
