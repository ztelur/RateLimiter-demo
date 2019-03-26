/**
 * Superid.menkor.com Inc.
 * Copyright (c) 2012-2019 All Rights Reserved.
 */
package com.remcarpediem.limiter.guavademo.redis;

import com.google.common.math.LongMath;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author libing
 * @version $Id: SmoothRateLimiter.java, v 0.1 2019年03月26日 下午7:36 zt Exp $
 */
abstract class SmoothRateLimiter extends RateLimiter {

    static final class SmoothWarmingUp extends SmoothRateLimiter {
        private final long warmupPeriodMicros;
        private double slope;
        private double thresholdPermits;
        private double coldFactor;

        SmoothWarmingUp(
                SleepingStopwatch stopwatch, long warmupPeriod, TimeUnit timeUnit, double coldFactor) {
            super(stopwatch);
            this.warmupPeriodMicros = timeUnit.toMicros(warmupPeriod);
            this.coldFactor = coldFactor;
        }

        @Override
        long storedPermitsToWaitTime(double storedPermits, double permitsToTake) {
            double availablePermitsAboveThreshold = redisPermits.getStoredPermits() - thresholdPermits;
            long micros = 0;
            if (availablePermitsAboveThreshold > 0.0) {
                double permitsAboveThresholdToTake = Math.min(availablePermitsAboveThreshold, permitsToTake);
                double length = permitsToTime(availablePermitsAboveThreshold) +
                        permitsToTime(availablePermitsAboveThreshold - permitsAboveThresholdToTake);
                micros = (long) (permitsAboveThresholdToTake * length / 2.0);
                permitsToTake -= permitsAboveThresholdToTake;
            }
            micros += (long) (stableIntervalMicros * permitsToTake);
            return micros;
        }
        private double permitsToTime(double permits) {
            return stableIntervalMicros + permits * slope;
        }

        @Override
        double coolDownIntervalMicros() {
            return warmupPeriodMicros / redisPermits.getMaxPermits();
        }

        @Override
        void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
            double oldMaxPermits = redisPermits.getMaxPermits();
            double coldIntervalMicros = stableIntervalMicros * coldFactor;
            thresholdPermits = 0.5 * warmupPeriodMicros / stableIntervalMicros;
            redisPermits.setMaxPermits(thresholdPermits + 2.0 * warmupPeriodMicros / (stableIntervalMicros + coldIntervalMicros));
            slope = (coldIntervalMicros - stableIntervalMicros) / (redisPermits.getMaxPermits() - thresholdPermits);
            if (oldMaxPermits == Double.POSITIVE_INFINITY) {
                redisPermits.setStoredPermits(0.0);
            } else {
                redisPermits.setStoredPermits((oldMaxPermits == 0.0) ? redisPermits.getMaxPermits() : redisPermits.getStoredPermits() * redisPermits.getMaxPermits() / oldMaxPermits);
            }
        }
    }


    static final class SmoothBursty extends SmoothRateLimiter {
        final double maxBurstSeconds;

        public SmoothBursty(SleepingStopwatch stopwatch, double maxBurstSeconds) {
            super(stopwatch);
            this.maxBurstSeconds = maxBurstSeconds;
        }

        @Override
        void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
            double oldMaxPermits = redisPermits.getMaxPermits();
            redisPermits.setMaxPermits(maxBurstSeconds * permitsPerSecond);
            if (oldMaxPermits == Double.POSITIVE_INFINITY) {
                redisPermits.setStoredPermits(redisPermits.getMaxPermits());
            } else {
                redisPermits.setStoredPermits((oldMaxPermits == 0.0) ? 0.0 : redisPermits.getStoredPermits() * redisPermits.getMaxPermits() / oldMaxPermits);
            }
        }

        @Override
        long storedPermitsToWaitTime(double storedPermits, double permitsToTake) {
            return 0L;
        }

        @Override
        double coolDownIntervalMicros() {
            return stableIntervalMicros;
        }
    }

    public SmoothRateLimiter(SleepingStopwatch stopwatch) {
        super(stopwatch);
    }

    @Override
    void doSetRate(double permitsPerSecond, long nowMicros) {
        redisPermits.reSync(nowMicros);
        double stableIntervalMicros = TimeUnit.SECONDS.toMicros(1L) / permitsPerSecond;
        this.stableIntervalMicros = stableIntervalMicros;
        doSetRate(permitsPerSecond, stableIntervalMicros);
    }

    abstract void doSetRate(double permitsPerSecond, double stableIntervalMicros);

    @Override
    double doGetRate() {
        return TimeUnit.SECONDS.toMicros(1L) / stableIntervalMicros;
    }

    @Override
    long queryEarliestAvailable(long nowMicros) {
        return redisPermits.getNextFreeTicketMicros();
    }

    @Override
    long reserveEarliestAvailable(int permits, long nowMicros) {
        RedisPermits curRedisPermits = this.redisPermits;
        curRedisPermits.reSync(nowMicros);
        long returnValue = curRedisPermits.getNextFreeTicketMicros();
        double storedPermitsToSpend = Math.min(permits, curRedisPermits.getStoredPermits());

        double freshPermits = permits - storedPermitsToSpend; // 需要等待的令牌数

        long waitMicros = storedPermitsToWaitTime(curRedisPermits.getStoredPermits(), storedPermitsToSpend + (long) (freshPermits * stableIntervalMicros));

        curRedisPermits.setNextFreeTicketMicros(LongMath.checkedAdd(curRedisPermits.getNextFreeTicketMicros(), waitMicros));
        curRedisPermits.setStoredPermits(curRedisPermits.getStoredPermits() - storedPermitsToSpend);
        this.redisPermits = curRedisPermits;
        return returnValue;
    }

    abstract long storedPermitsToWaitTime(double storedPermits, double permitsToTake);
    abstract double coolDownIntervalMicros();
}