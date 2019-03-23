package com.remcarpediem.limiter.guavademo.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.serializer.StringRedisSerializer;

public class PermitsTemplate extends RedisTemplate<String, RedisPermits> {
    private ObjectMapper objectMapper = new ObjectMapper();


    public PermitsTemplate(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public PermitsTemplate() {
        setKeySerializer(new StringRedisSerializer());
        setValueSerializer(new RedisSerializer<RedisPermits>() {
            @Override
            public byte[] serialize(RedisPermits redisPermits) throws SerializationException {
                try {
                    return objectMapper.writeValueAsBytes(redisPermits);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public RedisPermits deserialize(byte[] bytes) throws SerializationException {
                try {
                    return objectMapper.readValue(bytes, RedisPermits.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
    }
}
