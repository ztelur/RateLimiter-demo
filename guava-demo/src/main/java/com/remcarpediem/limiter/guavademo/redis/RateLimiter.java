package com.remcarpediem.limiter.guavademo.redis;

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;
import com.google.common.util.concurrent.Uninterruptibles;
import org.redisson.api.RLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class RateLimiter {

    private Logger logger = LoggerFactory.getLogger(RateLimiter.class.getName());

    private String key;
    private Long permitsPerSecond;
    private Integer maxBurstSeconds = 60;
//    private
    private PermitsTemplate permitsTemplate;
    private RLock rLock;

    RedisPermits redisPermits;

    SleepingStopwatch stopwatch;

    double stableIntervalMicros;

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

    /**
     * acquires the given number of tokens from this {@code RateLimiter}, blocking until the request
     * can be granted. Tells the amount of time slept, if any
     * @param tokens
     * @return time spent sleeping to enforce rate, in millisencods; o if negative or zero
     */
    public Long acquire(Long tokens) {
        Long milliToWait = reserve(tokens);
        logger.info("acquire for {}ms {}", milliToWait, Thread.currentThread().getName());
        stopwatch.sleepMicrosUninterruptibly(milliToWait);
        return milliToWait;
    }

    public Long acquire() {
        return acquire(1L);
    }


    public Boolean tryAcquire(int token, Long timeout, TimeUnit timeUnit) {
        long timeoutMicros = Math.max(timeUnit.toMicros(timeout), 0);
        checkPermits(token);

        long microsToWait;

        try {
            rLock.lock();
            long nowMicros = stopwatch.readMicros();
            if (!canAcquire(nowMicros, timeoutMicros)) {
                return false;
            } else {
                microsToWait = reserveAndGetWaitLength(token);
            }
        } finally {
            rLock.unlock();
        }

        stopwatch.sleepMicrosUninterruptibly(microsToWait);
        return true;
    }

    private Long now() {
        return permitsTemplate.execute() ? System.currentTimeMillis();
    }


    private Long reserve(int token) throws IllegalArgumentException {
        //checkToken(token);
        // TODO: 这里可以根据redis和zk进行分布式锁
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

    private Long queryEarliestAvailable(long nowMicros) {
        //Long now = System.currentTimeMillis();
        //RedisPermits permits = this.redisPermits;
        //permits.reSync(now);
        //
        //Long storedPermitsToSpend = Math.min(token, permits.getStoredPermits()); //可以消耗的令牌树
        //Long freshPermits = token - storedPermitsToSpend; // 需要等待的令牌数
        //Long waitMills = freshPermits * permits.getIntervalMillis();
        //
        //return LongMath.checkedAdd(permits.getNextFreeTicketMillis() - now, waitMills);
        return redisPermits.getNextFreeTicketMicros();
    }

    private static void checkPermits(int permits) {
        Preconditions.checkArgument(permits > 0, "Requested permits (%s) must be positive", permits);
    }


    /**
     * 获取下一个ticket 然后返回必须等待的时间
     * @param requiredPermits
     * @return
     */
    private Long reserveAndGetWaitLength(int requiredPermits) {
        Long now = System.currentTimeMillis();
        RedisPermits permits = this.redisPermits;
        permits.reSync(now);
        long returnValue = permits.getNextFreeTicketMicros();
        double storedPermitsToSpend = Math.min(requiredPermits, permits.getStoredPermits());

        double freshPermits = requiredPermits - storedPermitsToSpend; // 需要等待的令牌数

        long waitMicros = storedPermitsToWaitTime(permits.getStoredPermits(), storedPermitsToSpend + (long) (freshPermits * stableIntervalMicros));

        permits.setNextFreeTicketMicros(LongMath.checkedAdd(permits.getNextFreeTicketMicros(), waitMicros));
        permits.setStoredPermits(permits.getStoredPermits() - storedPermitsToSpend);
        this.redisPermits = permits;
        return returnValue;
    }

    abstract long storedPermitsToWaitTime(double storedPermits, double permitsToTake);


    abstract static class SleepingStopwatch {
        protected SleepingStopwatch() {}

        protected abstract long readMicros();

        protected abstract void sleepMicrosUninterruptibly(long micros);


        public static SleepingStopwatch createFromSystemTimer() {
            return new SleepingStopwatch() {

                final Stopwatch stopwatch = Stopwatch.createStarted();

                @java.lang.Override
                protected long readMicros() {
                    return stopwatch.elapsed(TimeUnit.MICROSECONDS);
                }

                @java.lang.Override
                protected void sleepMicrosUninterruptibly(long micros) {
                    if (micros > 0) {
                        Uninterruptibles.sleepUninterruptibly(micros, TimeUnit.MICROSECONDS);
                    }
                }
            };
        }

    }

}
