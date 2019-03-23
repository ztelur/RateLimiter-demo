package com.remcarpediem.limiter.guavademo.configuration;

import com.google.common.util.concurrent.RateLimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RateLimiterConfig {
    @Bean
    public RateLimiter rateLimiter() {
        RateLimiter rateLimiter = RateLimiter.create(1);
        return rateLimiter;
    }
}
