package com.example.kafkaExercise.service;

import com.example.kafkaExercise.entity.AdEvaluationResult;
import com.example.kafkaExercise.repository.AdEvaluationResultRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;

// Claude 추가: AdEvaluationComplete 토픽을 구독해서, 카프카 안에서만 흐르던 광고 효과 결과를
// Postgres 테이블(AdEvaluationResult)에 영구 저장한다. String으로 그냥 받아서 ObjectMapper로
// 직접 파싱 -> ConsumerService(String 그대로 println하는 것)와 동일한 기본 컨테이너 팩토리를 쓰므로
// 별도 ConsumerFactory/JsonDeserializer 설정이 필요 없다(그 방식에서 예전에 겪었던 직렬화 설정
// 실수를 피하기 위해 일부러 String으로 받고 수동 파싱).
@Slf4j
@Service
@RequiredArgsConstructor
public class AdEvaluationResultListener {

    private final AdEvaluationResultRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "AdEvaluationComplete", groupId = "ad-eval-db-writer")
    public void consume(String message) {
        try {
            Map<String, Object> parsed = objectMapper.readValue(message, Map.class);

            AdEvaluationResult entity = new AdEvaluationResult();
            entity.setAdId((String) parsed.get("adId"));
            entity.setUserId((String) parsed.get("userId"));
            entity.setOrderId((String) parsed.get("orderId"));

            Object productInfo = parsed.get("productInfo");
            if (productInfo instanceof Map) {
                Map<?, ?> productInfoMap = (Map<?, ?>) productInfo;
                entity.setProductId((String) productInfoMap.get("productId"));
                entity.setPrice((String) productInfoMap.get("price"));
            }

            repository.save(entity);
            log.info("AdEvaluationComplete 결과 DB 저장 완료: {}", entity);
        } catch (Exception e) {
            log.error("AdEvaluationComplete 메시지 파싱/저장 실패: {}", message, e);
        }
    }
}
