package com.remcarpediem.limiter.guavademo.redis;

import java.util.concurrent.TimeUnit;

public class RedisPermits {
    private Long maxPermits;
    private Long storedPermits;
    private Long intervalMillis;
    private Long nextFreeTicketMillis;


    public RedisPermits(Long permitsPerSecond, Integer maxBurstSeconds, Long nextFreeTicketMillis) {
        this.maxPermits = (permitsPerSecond * maxBurstSeconds);
        this.storedPermits = permitsPerSecond;
        this.intervalMillis =  new Double(TimeUnit.SECONDS.toMillis(1) / (permitsPerSecond * 1.0)).longValue();
        this.nextFreeTicketMillis = nextFreeTicketMillis;
    }

    public Long expires() {
        long now = System.currentTimeMillis();
        return 2 * TimeUnit.MINUTES.toSeconds(1) + TimeUnit.MILLISECONDS.toSeconds(Math.max(nextFreeTicketMillis, now) - now)
        return 1L;
    }

    public Boolean reSync(Long now) {
        if (now > nextFreeTicketMillis) {
            storedPermits = Math.min(maxPermits, storedPermits + (now - nextFreeTicketMillis) / intervalMillis);
            nextFreeTicketMillis = now;
            return true;
        }
        return false;
    }


    public Long getMaxPermits() {
        return maxPermits;
    }

    public void setMaxPermits(Long maxPermits) {
        this.maxPermits = maxPermits;
    }

    public Long getStoredPermits() {
        return storedPermits;
    }

    public void setStoredPermits(Long storedPermits) {
        this.storedPermits = storedPermits;
    }

    public Long getIntervalMillis() {
        return intervalMillis;
    }

    public void setIntervalMillis(Long intervalMillis) {
        this.intervalMillis = intervalMillis;
    }

    public Long getNextFreeTicketMillis() {
        return nextFreeTicketMillis;
    }

    public void setNextFreeTicketMillis(Long nextFreeTicketMillis) {
        this.nextFreeTicketMillis = nextFreeTicketMillis;
    }
}
