package com.remcarpediem.limiter.guavademo.redis;

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;
import io.lettuce.core.RedisClient;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RateLimiter {

    private Logger logger = LoggerFactory.getLogger(RateLimiter.class.getName());

    private String key;
    private Long permitsPerSecond;
    private Integer maxBurstSeconds = 60;
//    private
    private PermitsTemplate permitsTemplate;
    private RLock rLock;

    public RedisPermits redisPermits;

    /**
     * 生成并且默认存储令牌桶
     * @return
     */
    private RedisPermits putDefaultPermits() {
        RedisPermits redisPermits = new RedisPermits(permitsPerSecond, maxBurstSeconds, System.currentTimeMillis());
        permitsTemplate.opsForValue().set(key, redisPermits, redisPermits.expires(), TimeUnit.SECONDS);
        return redisPermits;
    }

    /**
     * 获取更新令牌桶
     * @return
     */
    private RedisPermits permits() {
        RedisPermits permits = permitsTemplate.opsForValue().get(key);
        if (permits == null) {
            permits = putDefaultPermits();
        }
        permitsTemplate.opsForValue().set(key, permits, permits.expires(), TimeUnit.SECONDS);
        return permits;
    }



    public Long acquire(Long tokens) {
        Long milliToWait = reserve(tokens);
        logger.info("acquire for {}ms {}", milliToWait, Thread.currentThread().getName());
        Thread.sleep(milliToWait);
        return milliToWait;
    }

    public Long acquire() {
        return acquire(1L);
    }


    public Boolean tryAcquire(Long token, Long timeout, TimeUnit timeUnit) {
        Long timeoutMicros = Math.max(timeUnit.toMillis(timeout), 0);
        checkToken(token);

        Long milliToWait;

        try {
            rLock.lock();
            if (!canAcquire(token, timeoutMicros)) {
                return false;
            } else {
                milliToWait = reserveAndGetWaitLength(token);
            }
        } finally {
            rLock.unlock();
        }

        Thread.sleep(milliToWait);
        return true;
    }

    private Long now() {
        return permitsTemplate.execute() ? System.currentTimeMillis();
    }


    private Long reserve(Long token) throws IllegalArgumentException {
        checkToken(token);
        try {
            rLock.lock();
            return reserveAndGetWaitLength(token);
        } finally {
            rLock.unlock();
        }
    }

    private void checkToken(Long token) {
        Preconditions.checkArgument(token > 0, "Requested tokens $tokens must be positive");
    }

    private Boolean canAcquire(Long token, Long timeoutMillis) {
        return queryEarliestAvailable(token) - timeoutMillis <= 0;
    }

    private Long queryEarliestAvailable(Long token) {
        Long now = System.currentTimeMillis();
        RedisPermits permits = this.redisPermits;
        permits.reSync(now);

        Long storedPermitsToSpend = Math.min(token, permits.getStoredPermits()); //可以消耗的令牌树
        Long freshPermits = token - storedPermitsToSpend; // 需要等待的令牌数
        Long waitMills = freshPermits * permits.getIntervalMillis();

        return LongMath.checkedAdd(permits.getNextFreeTicketMillis() - now, waitMills);
    }


    /**
     * 获取下一个ticket 然后返回必须等待的时间
     * @param token
     * @return
     */
    private Long reserveAndGetWaitLength(Long token) {
        Long now = System.currentTimeMillis();
        RedisPermits permits = this.redisPermits;
        permits.reSync(now);

        Long storedPermitsToSpend = Math.min(token, permits.getStoredPermits()); //可以消耗的令牌树
        Long freshPermits = token - storedPermitsToSpend; // 需要等待的令牌数
        Long waitMills = freshPermits * permits.getIntervalMillis();
        permits.setNextFreeTicketMillis(LongMath.checkedAdd(permits.getNextFreeTicketMillis(), waitMills));
        permits.setStoredPermits(permits.getStoredPermits() - storedPermitsToSpend);
        this.redisPermits = permits;

        return permits.getNextFreeTicketMillis() - now;
    }


}
