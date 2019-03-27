package com.remcarpediem.limiter.guavademo.redis;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import com.remcarpediem.limiter.guavademo.redis.SmoothRateLimiter.SmoothBursty;
import org.redisson.api.RLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class RateLimiter {

    private Logger logger = LoggerFactory.getLogger(RateLimiter.class.getName());


    public static RateLimiter create(double permitsPerSecond) {

    }

    static RateLimiter create(double permitsPerSecond, SleepingStopwatch stopwatch) {
        RateLimiter rateLimiter = new SmoothBursty(stopwatch, 1.0);
        rateLimiter.setRate(permitsPerSecond);
        return rateLimiter;
    }

    public static RateLimiter create(double permitsPerSecond, long warmupPeriod, TimeUnit unit) {
        checkArgument(warmupPeriod >= 0, "warmupPeriod must not be negative: %s", warmupPeriod);
        return create(
                permitsPerSecond, warmupPeriod, unit, 3.0, SleepingStopwatch.createFromSystemTimer());
    }


    static RateLimiter create(
            double permitsPerSecond,
            long warmupPeriod,
            TimeUnit unit,
            double coldFactor,
            SleepingStopwatch stopwatch) {
        RateLimiter rateLimiter = new SmoothRateLimiter.SmoothWarmingUp(stopwatch, warmupPeriod, unit, coldFactor);
        rateLimiter.setRate(permitsPerSecond);
        return rateLimiter;
    }


    private String key;
    private Long permitsPerSecond;
    private Integer maxBurstSeconds = 60;
//    private
    private PermitsTemplate permitsTemplate;
    private RLock rLock;

    RedisPermits redisPermits;

    SleepingStopwatch stopwatch;

    double stableIntervalMicros;

    RateLimiter(SleepingStopwatch stopwatch) {
        this.stopwatch = checkNotNull(stopwatch);
    }


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


    public double acquire() {
        return acquire(1);
    }


    public final void setRate(double permitsPerSecond) {
        Preconditions.checkArgument(permitsPerSecond > 0.0 && !Double.isNaN(permitsPerSecond), "rate must be positive");
        try {
            rLock.lock();
            doSetRate(permitsPerSecond, stopwatch.readMicros());
        } finally {
            rLock.unlock();
        }
    }

    abstract void doSetRate(double permitsPerSecond, long nowMicros);


    public final double getRate() {
        try {
            rLock.lock();
            return doGetRate();
        } finally {
            rLock.unlock();
        }
    }

    abstract double doGetRate();


    /**
     * acquires the given number of tokens from this {@code RateLimiter}, blocking until the request
     * can be granted. Tells the amount of time slept, if any
     * @param tokens
     * @return time spent sleeping to enforce rate, in millisencods; o if negative or zero
     */
    public double acquire(int tokens) {
        Long milliToWait = reserve(tokens);
        logger.info("acquire for {}ms {}", milliToWait, Thread.currentThread().getName());
        stopwatch.sleepMicrosUninterruptibly(milliToWait);
        return milliToWait;
    }


    public Boolean tryAcquire(int permits, long timeout, TimeUnit timeUnit) {
        long timeoutMicros = Math.max(timeUnit.toMicros(timeout), 0);
        checkPermits(permits);

        long microsToWait;

        try {
            rLock.lock();
            long nowMicros = stopwatch.readMicros();
            if (!canAcquire(nowMicros, timeoutMicros)) {
                return false;
            } else {
                microsToWait = reserveAndGetWaitLength(permits, nowMicros);
            }
        } finally {
            rLock.unlock();
        }

        stopwatch.sleepMicrosUninterruptibly(microsToWait);
        return true;
    }

    private boolean canAcquire(long nowMicros, long timeoutMicros) {
        return queryEarliestAvailable(nowMicros) - timeoutMicros <= nowMicros;
    }



    //private Long now() {
    //    return permitsTemplate.execute() ? System.currentTimeMillis();
    //}


    private Long reserve(int permits) throws IllegalArgumentException {
        checkToken(permits);
        // TODO: 这里可以根据redis和zk进行分布式锁
        try {
            rLock.lock();
            return reserveAndGetWaitLength(permits, stopwatch.readMicros());
        } finally {
            rLock.unlock();
        }
    }

    private void checkToken(int token) {
        Preconditions.checkArgument(token > 0, "Requested tokens $tokens must be positive");
    }

    private Boolean canAcquire(Long token, Long timeoutMillis) {
        return queryEarliestAvailable(token) - timeoutMillis <= 0;
    }


    private static void checkPermits(int permits) {
        Preconditions.checkArgument(permits > 0, "Requested permits (%s) must be positive", permits);
    }


    private Long reserveAndGetWaitLength(int permits, long nowMicros) {
        long momentAvailable = reserveEarliestAvailable(permits, nowMicros);

        return Math.max(momentAvailable - nowMicros, 0);
    }

    abstract long reserveEarliestAvailable(int permits, long nowMicros);
    abstract long queryEarliestAvailable(long nowMicros);


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
