package com.remcarpediem.limiter.guavademo.controller;

import com.remcarpediem.limiter.guavademo.service.DemoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DemoController {
    @Autowired
    private DemoService demoService;

    @GetMapping("/test")
    public Long getId() {
        return demoService.getId();
    }
}
