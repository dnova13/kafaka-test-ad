package com.example.kafkaExercise.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// Claude 추가: AdEvaluationComplete 토픽으로 나온 광고 효과 조인 결과를 저장하는 테이블.
// EffectOrNot의 productInfo(Map)는 관계형 테이블에 그대로 담기 애매해서 productId/price로 풀어서 저장.
@Entity
@Data
@NoArgsConstructor
public class AdEvaluationResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    String adId;
    String userId;
    String orderId;
    String productId;
    String price;

    // 이 row가 DB에 저장된 시각 (카프카 메시지 자체 타임스탬프가 아니라 저장 시점)
    LocalDateTime receivedAt = LocalDateTime.now();
}
