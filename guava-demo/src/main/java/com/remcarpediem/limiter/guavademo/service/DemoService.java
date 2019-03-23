package com.remcarpediem.limiter.guavademo.service;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DemoService {
    private Logger logger = LoggerFactory.getLogger(DemoService.class.getName());
    @Autowired
    private RateLimiter rateLimiter;
    public Long getId() {
        Double waitTime = rateLimiter.acquire(1);
        logger.info("cur time is " + System.currentTimeMillis() / 1000 + " wait time is " + waitTime);
        return 1L;
    }
}
