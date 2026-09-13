package com.example.kafkaExercise.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KTableService {

    // Claude 추가(상세 주석): StreamService와 똑같은 leftTopic/rightTopic을 소스로 쓰지만,
    // sb.stream(...).toTable()로 KStream을 KTable로 변환해서 join하는 KTable-KTable 조인 데모.
    // StreamService의 KStream-KStream 조인(윈도우 필요)과 비교하기 위한 코드로 보인다.
    //
    // 지금 시점 기준 주석 해제해도 되는지: 된다. leftTopic/rightTopic은 KafkaConfig의
    // NewTopic 빈으로 이미 생성돼 있어서 MissingSourceTopicException은 안 난다.
    //
    // 다만 주의할 점 두 가지:
    // 1) StreamService도 같은 leftTopic/rightTopic을 구독하고 있어서, 이걸 같이 켜면
    //    하나의 공유 Topology 안에 같은 토픽을 소스로 하는 소스 노드가 두 개(KStream용 하나,
    //    KTable용 하나) 생긴다. 에러는 안 나지만 같은 메시지가 두 갈래로 각각 처리된다.
    // 2) 결과를 똑같이 "joinedMsg" 토픽으로 내보내기 때문에, StreamService의
    //    [StringJoiner]/[StringOuterJoiner] 결과와 이 KTableService의 [StringJoiner] 결과가
    //    같은 토픽에 섞여서 쌓인다 — 조회할 때 어느 클래스가 낸 결과인지 태그만으로는
    //    완전히 구분이 안 될 수 있으니(둘 다 "[StringJoiner]" 접두사 사용) 헷갈리지 않게 주의.
    //
    // KTable-KTable join은 KStream-KStream과 달리 JoinWindows(시간창)가 필요 없다.
    // KTable은 "각 key의 최신 상태"를 계속 들고 있는 구조라서, 조인 시점에 양쪽 다
    // 최신값이 존재하기만 하면 시간 제약 없이 바로 매칭된다.
    //
    // 예시 1) 정상 매칭 - 시간 제약 없음
    //   leftTopic  : key=1, value=leftValue   (t=0초)
    //   rightTopic : key=1, value=rightValue  (t=100초, 한참 뒤에 보내도 상관없음)
    //   -> joinedMsg 결과: [StringJoiner]leftValue-rightValue
    //   (StreamService의 KStream-KStream join이었다면 10초 넘어서 매칭 자체가 안 됐을 상황)
    //
    // 예시 2) 같은 key로 값이 업데이트된 경우 - 최신 값끼리만 매칭됨
    //   leftTopic : key=1, value=A  (t=0초)
    //   leftTopic : key=1, value=B  (t=1초, 같은 key로 값이 갱신됨 -> KTable 상태가 A에서 B로 덮어써짐)
    //   rightTopic: key=1, value=X  (t=2초)
    //   -> joinedMsg 결과: [StringJoiner]B-X   (A는 이미 덮어써져서 조인에 안 쓰임)
    //
    // 예시 3) 한쪽 key가 아예 없는 경우 - 결과 자체가 안 나옴 (KStream의 outerJoin과 달리
    //   KTable.join()은 기본이 inner join이라, 매칭 안 되면 null로라도 채워서 내보내주지 않고
    //   그냥 아무 결과도 안 낸다. outer join을 하려면 leftTable.outerJoin(rightTable, ...) 사용)
    //   leftTopic : key=2, value=onlyLeft
    //   rightTopic: (key=2 없음)
    //   -> joinedMsg 결과: 없음
    @Autowired
    public void buildPipeline(StreamsBuilder sb) {

        // leftTopic/rightTopic을 KStream으로 구독한 뒤 .toTable()로 KTable로 변환.
        // KTable은 내부적으로 key별 최신 값만 유지하는 상태 저장소(state store)를 갖는다.
        KTable<String, String> leftTable = sb.stream("leftTopic", Consumed.with(Serdes.String(),Serdes.String())).toTable();
        KTable<String, String> rightTable = sb.stream("rightTopic", Consumed.with(Serdes.String(),Serdes.String())).toTable();

        // 두 값을 합치는 함수 — StreamService의 inner join용 조인 함수와 동일한 포맷.
        ValueJoiner<String, String, String> stringJoiner = (leftValue, rightValue) -> {
            return "[StringJoiner]" + leftValue + "-" + rightValue;
        };

        // 시간창 없이 바로 join. 같은 key로 양쪽 다 최신 값이 있으면 즉시 매칭됨
        // (StreamService처럼 "10초 이내에 보내야 한다" 같은 제약이 없다).
        KTable<String, String> joinedTable = leftTable.join(rightTable, stringJoiner);
        // KTable 조인 결과를 다시 KStream으로 바꿔서(toStream) joinedMsg 토픽으로 produce.
        joinedTable.toStream().to("joinedMsg");
    }
}
