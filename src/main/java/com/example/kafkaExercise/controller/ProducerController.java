package com.example.kafkaExercise.controller;

import com.example.kafkaExercise.entity.AdEvaluationResult;
import com.example.kafkaExercise.repository.AdEvaluationResultRepository;
import com.example.kafkaExercise.service.AdEvaluationService;
import com.example.kafkaExercise.service.Producer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class ProducerController {

    private final Producer producer;
    // Claude 추가: 프론트 페이지용 - adLog/purchaseLog produce, AdEvaluationComplete 결과 조회
    private final AdEvaluationResultRepository adEvaluationResultRepository;
    // Claude 추가: sendNewMsg() 호출용 (랜덤 테스트 데이터 생성)
    private final AdEvaluationService adEvaluationService;

    @PostMapping("/message")
    public void PublishMessage(@RequestParam String msg) {
        producer.pub(msg);
    }

    // Claude 추가: 순수 문자열 테스트용 엔드포인트
    @PostMapping("/smp-msg")
    public void PublishSimpleMessage(@RequestParam String msg) {
        producer.pubString(msg);
    }

    // Claude 추가: 프론트 페이지에서 adLog JSON을 그대로 받아 adLog 토픽으로 produce
    @PostMapping("/adLog")
    public void publishAdLog(@RequestBody String json) {
        producer.sendRawJson("adLog", json);
    }

    // Claude 추가: 프론트 페이지에서 purchaseLog JSON을 그대로 받아 purchaseLog 토픽으로 produce
    @PostMapping("/purchaseLog")
    public void publishPurchaseLog(@RequestBody String json) {
        producer.sendRawJson("purchaseLog", json);
    }

    // Claude 추가: AdEvaluationComplete 결과를 DB에서 최신순으로 조회해서 프론트에 뿌려줌
    @GetMapping("/ad-evaluation-results")
    public List<AdEvaluationResult> getAdEvaluationResults() {
        return adEvaluationResultRepository.findAllByOrderByReceivedAtDesc();
    }

    // Claude 추가: 어디에서도 호출 안 되던 AdEvaluationService.sendNewMsg()를 연결.
    // 호출할 때마다 매칭되는 adLog+purchaseLog 한 쌍을 랜덤 값으로 생성해서 카프카에 produce함.
    @PostMapping("/gen-test-data")
    public void generateTestData() {
        adEvaluationService.sendNewMsg();
    }
}
