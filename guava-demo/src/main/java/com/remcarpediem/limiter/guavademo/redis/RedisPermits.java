package com.remcarpediem.limiter.guavademo.redis;

import java.util.concurrent.TimeUnit;

public abstract class RedisPermits {
    private double maxPermits;
    private double storedPermits;
    private Long intervalMillis;
    private Long nextFreeTicketMicros = 0L;


    public RedisPermits(Long permitsPerSecond, Integer maxBurstSeconds, Long nextFreeTicketMillis) {
        this.maxPermits = (permitsPerSecond * maxBurstSeconds);
        this.storedPermits = permitsPerSecond;
        this.intervalMillis =  new Double(TimeUnit.SECONDS.toMillis(1) / (permitsPerSecond * 1.0)).longValue();
        this.nextFreeTicketMicros = nextFreeTicketMillis;
    }

    public Long expires() {
        long now = System.currentTimeMillis();
        return 2 * TimeUnit.MINUTES.toSeconds(1) + TimeUnit.MILLISECONDS.toSeconds(Math.max(nextFreeTicketMicros, now) - now);
        return 1L;
    }

    public Boolean reSync(Long nowMicros) {
        if (nowMicros > nextFreeTicketMicros) {
            double newPermits =  (nowMicros - nextFreeTicketMicros) / coolDownIntervalMicros();
            storedPermits = Math.min(maxPermits, storedPermits + newPermits);
            nextFreeTicketMicros = nowMicros;
            return true;
        }
        return false;
    }

    abstract double coolDownIntervalMicros();


    public double getMaxPermits() {
        return maxPermits;
    }

    public void setMaxPermits(Long maxPermits) {
        this.maxPermits = maxPermits;
    }

    public double getStoredPermits() {
        return storedPermits;
    }

    public void setStoredPermits(double storedPermits) {
        this.storedPermits = storedPermits;
    }

    public double getIntervalMillis() {
        return intervalMillis;
    }

    public void setIntervalMillis(Long intervalMillis) {
        this.intervalMillis = intervalMillis;
    }

    public long getNextFreeTicketMicros() {
        return nextFreeTicketMicros;
    }

    public void setNextFreeTicketMicros(Long nextFreeTicketMicros) {
        this.nextFreeTicketMicros = nextFreeTicketMicros;
    }
}
