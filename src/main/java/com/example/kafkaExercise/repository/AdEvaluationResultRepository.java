package com.example.kafkaExercise.repository;

import com.example.kafkaExercise.entity.AdEvaluationResult;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

// Claude 추가: Spring Data JPA가 메서드 선언만으로 SQL을 자동 생성해줌 (findAll, save 등은 기본 제공)
public interface AdEvaluationResultRepository extends JpaRepository<AdEvaluationResult, Long> {
    List<AdEvaluationResult> findAllByOrderByReceivedAtDesc();
}
