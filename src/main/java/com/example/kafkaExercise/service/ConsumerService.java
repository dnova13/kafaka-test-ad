package com.example.kafkaExercise.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    @KafkaListener(topics ="defaultTopic", groupId = "foo")
    public void consumer (String message) {
        System.out.println(String.format("### Subscribed111 :  %s", message));
    }
}