package com.example.kafkaExercise.service;

import com.example.kafkaExercise.config.KafkaConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class Producer {

//    @Autowired
//    KafkaConfig myConfig;

    private final KafkaConfig myConfig;

    String topicName = "defaultTopic";

    private KafkaTemplate<String, Object> kafkaTemplate;


//    public Producer(KafkaTemplate kafkaTemplate) {
//        this.kafkaTemplate = kafkaTemplate;
//    }

    public void pub(String msg) {
        kafkaTemplate = myConfig.KafkaTemplateForGeneral();
        kafkaTemplate.send(topicName, msg);
    }

    // Claude 추가: pub()은 value serializer가 PurchaseLogOneProductSerializer라 순수 String을 보내면
    // ClassCastException(500 에러)이 나서, StringSerializer 기반 템플릿으로 보내는 메서드를 따로 뺌
    public void pubString(String msg) {
        myConfig.KafkaTemplateForString().send(topicName, msg);
    }

    public void sendJoinedMsg(String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForGeneral();
        kafkaTemplate.send(topicNm, msg);
    }

    public void sendMsgForWatchingAdLog(String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForWatchingAdLog();
        kafkaTemplate.send(topicNm, msg);
    }

    public void sendMsgForPurchaseLog(String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForPurchaseLog();
        kafkaTemplate.send(topicNm, msg);
    }

    // Claude 추가: 프론트 페이지에서 받은 raw JSON 문자열을 그대로 원하는 토픽으로 produce.
    // AdEvaluationService 쪽에서 JsonDeserializer(대상클래스)로 바이트를 읽기 때문에,
    // 여기서 보내는 문자열이 그 클래스 필드 구조와 맞는 JSON이기만 하면 정상 동작함
    // (프로듀서/컨슈머가 서로 다른 Serde 객체를 써도 최종적으로 UTF-8 JSON 바이트만 맞으면 문제없음).
    public void sendRawJson(String topicNm, String json) {
        myConfig.KafkaTemplateForString().send(topicNm, json);
    }


}
