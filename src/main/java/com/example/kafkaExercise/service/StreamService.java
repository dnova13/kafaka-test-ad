package com.example.kafkaExercise.service;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class StreamService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    // Claude 추가(상세 주석): @EnableKafkaStreams가 만들어주는 공유 StreamsBuilder 빈을
    // 메서드 파라미터로 주입받아, 여기서 만든 모든 스트림 정의가 앱 전체에서 공유하는
    // 하나의 Topology(=하나의 KafkaStreams 클라이언트)에 합쳐진다. 즉 이 클래스뿐 아니라
    // KTableService, AdEvaluationService의 buildPipeline도 전부 같은 토폴로지에 얹힌다 —
    // 그래서 이 클래스가 구독하는 소스 토픽(leftTopic/rightTopic 등)이 없으면
    // 다른 서비스(AdEvaluationService)의 스트림까지 전부 같이 죽는다.
    @Autowired
    public void buildPipeline(StreamsBuilder sb) {

        // --- 데모 1: 필터링 (fastcampus -> freeClassList) ---
        // fastcampus 토픽을 String key/value로 구독. Consumed.with(첫번째=key타입, 두번째=value타입).
        KStream<String, String> myStream = sb.stream("fastcampus", Consumed.with(STRING_SERDE, STRING_SERDE));

        // 들어오는 모든 레코드를 콘솔(System.out)에 그대로 출력. 필터링 여부와 무관하게
        // myStream이라는 같은 소스에서 갈라진 별개의 분기라서, 아래 filter 결과와 상관없이
        // fastcampus로 들어오는 건 전부 다 찍힌다. 출력 형식은 "key, value"인데,
        // kafka-console-producer로 key 없이 값만 보내면 key 자리는 항상 null로 찍힌다.
        myStream.print(Printed.toSysOut());

        // value(메시지 내용)에 "freeClass" 문자열이 포함된 것만 걸러서 freeClassList 토픽으로 produce.
        // 조건에 안 맞는 메시지는 버려지고 어디로도 안 나감(위 print와는 별개 분기이므로
        // 콘솔에는 필터링 전 상태로 여전히 찍힘).
        // kafka-console-consumer --topic freeClassList --from-beginning 통해 필터되어서 해당 토픽에 들어가는거 볼 수 있음.ㄹㄴㅁㅇ;ㅓ
        //
        // Claude 추가(예시): 필터 데모 결과값
        //   입력(fastcampus): "오늘 freeClass 특강 안내", "그냥 일반 공지사항"
        //   -> 콘솔(print) 출력: 둘 다 찍힘 (null, 오늘 freeClass 특강 안내 / null, 그냥 일반 공지사항)
        //   -> freeClassList 토픽 결과: "오늘 freeClass 특강 안내"만 들어감 ("freeClass" 미포함은 버려짐)
        myStream.filter((key, value)-> value.contains("freeClass")).to("freeClassList");

        // --- 데모 2: KStream-KStream 윈도우 조인 (leftTopic + rightTopic -> joinedMsg) ---
        // 서로 다른 두 토픽을 각각 스트림으로 구독. 예시 데이터는 "1:leftValue", "1:rightValue"처럼
        // 같은 key(1)로 들어온다고 가정 — 조인은 key가 같아야 매칭된다.
        KStream<String, String> leftStream = sb.stream("leftTopic",
                Consumed.with(STRING_SERDE, STRING_SERDE));
        // key:value --> 1:leftValue
        KStream<String, String> rightStream = sb.stream("rightTopic",
                Consumed.with(STRING_SERDE, STRING_SERDE));
        // key:value --> 1:rightValue

        leftStream.print(Printed.toSysOut());
        rightStream.print(Printed.toSysOut());

        // 두 값을 어떻게 합칠지 정의하는 함수(ValueJoiner). inner join용.
        ValueJoiner<String, String, String> stringJoiner = (leftValue, rightValue) -> {
            return "[StringJoiner]" + leftValue + "-" + rightValue;
        };

        // outer join용 조인 함수. 매칭 안 된 쪽 값은 null로 들어올 수 있음.
        ValueJoiner<String, String, String> stringOuterJoiner = (leftValue, rightValue) -> {
            return "[StringOuterJoiner]" + leftValue + "<" + rightValue;
        };

        // Inner join: 같은 key의 leftStream/rightStream 레코드가 10초 이내에 "둘 다" 도착해야만
        // 결과가 나온다. 한쪽만 오면 아무 결과도 안 나옴. KStream-KStream join은 시간창(JoinWindows)이
        // 필수 — 스트림은 테이블과 달리 "지금 이 순간의 전체 상태"가 없어서, 언제까지 짝을 기다릴지
        // 명시적으로 정해줘야 하기 때문(참고: AdEvaluationService의 KTable-KTable join은 이게 불필요).
        KStream<String, String> joinedStream = leftStream.join(rightStream,
                stringJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)));

        // Outer join: 10초 이내에 짝이 안 맞아도(한쪽만 와도) 결과가 나옴 — 못 만난 쪽 값은 null로 채워짐.
        KStream<String, String> outerJoinedStream = leftStream.outerJoin(rightStream,
                stringOuterJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)));

        // Claude 추가(예시): 조인 데모 결과값 (joinedMsg 토픽 기준, 실제 테스트로 확인된 결과)
        //
        //   예시 1) 정상 매칭 - key 같음, 10초 이내
        //     leftTopic  : key=1, value=leftValue   (t=0초)
        //     rightTopic : key=1, value=rightValue  (t=3초)
        //     -> 결과: [StringJoiner]leftValue-rightValue
        //             [StringOuterJoiner]leftValue<rightValue
        //
        //   예시 2) 한쪽만 옴 - inner join은 결과 없음, outer join만 null로 채워서 나옴
        //     leftTopic  : key=l, value=1212  (t=0초, rightTopic엔 key=l 없음)
        //     -> 결과: [StringOuterJoiner]1212<null   ([StringJoiner]... 는 안 나옴)
        //
        //   예시 3) key가 서로 다름 - 매칭 자체가 안 되고 각자 outer join으로만 처리됨
        //     leftTopic  : key=l, value=1212
        //     rightTopic : key=r, value=212121
        //     -> 결과: [StringOuterJoiner]1212<null, [StringOuterJoiner]null<212121
        //
        //   예시 4) 10초 초과 - 둘 다 outer join으로만 처리되고 inner join 결과는 없음
        //     leftTopic  : key=1, value=leftValue   (t=0초)
        //     rightTopic : key=1, value=rightValue  (t=15초, 윈도우 10초 초과)
        //     -> 결과: [StringOuterJoiner]leftValue<null, [StringOuterJoiner]null<rightValue
        //
        //   참고: 결과가 바로 안 뜨고 최대 commit.interval.ms(기본 30초) 만큼 지연될 수 있고,
        //   앱 재시작(at-least-once 재처리) 시 같은 결과가 중복 출력될 수 있음.

        System.out.println("################## lllll joinedStream = " + joinedStream);
        // inner join 결과만 콘솔에 출력(비교용 — outer join 결과는 콘솔에 안 찍힘).
        joinedStream.print(Printed.toSysOut());
        // inner/outer join 결과 둘 다 같은 joinedMsg 토픽으로 produce.
        joinedStream.to("joinedMsg");


        System.out.println("################## ooooooooooooo joinedStream = " + joinedStream);
        outerJoinedStream.print(Printed.toSysOut());
        outerJoinedStream.to("joinedMsg");
    }
}
