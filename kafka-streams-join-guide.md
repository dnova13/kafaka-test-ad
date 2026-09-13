# Kafka Streams Join 정리

이 프로젝트(`StreamService`, `KTableService`, `AdEvaluationService`)에서 실제로 쓰인 조인 3종류를 정리한다. 전부 **직접 테스트해서 실제 결과값까지 확인**한 내용이다.

## 공통 전제: co-partitioning

조인하려는 두 토픽은 **파티션 개수가 같아야** 한다(co-partitioning). 파티션 수가 다르면 어느 브로커의 어느 파티션에 있는 key가 서로 매칭되는지 보장이 안 되기 때문. 이 프로젝트에서는 `leftTopic`/`rightTopic`, `adLog`/`purchaseLog`/`purchaseLogOneProduct` 전부 파티션 3개로 통일해뒀다.

또한 **key가 없으면(null) 조인 대상에서 조용히 제외**된다 (에러 없이 그냥 버려짐). 조인은 항상 key를 기준으로 매칭한다.

---

## 1. KStream-KStream Join (윈도우 조인) — `StreamService.java`

스트림 대 스트림 조인. **시간창(JoinWindows)이 필수**다 — 스트림은 "지금 이 순간의 전체 상태"라는 개념이 없어서, 언제까지 짝을 기다릴지 명시적으로 정해줘야 한다.

```java
KStream<String, String> leftStream = sb.stream("leftTopic", Consumed.with(STRING_SERDE, STRING_SERDE));
KStream<String, String> rightStream = sb.stream("rightTopic", Consumed.with(STRING_SERDE, STRING_SERDE));

ValueJoiner<String, String, String> stringJoiner = (leftValue, rightValue) ->
        "[StringJoiner]" + leftValue + "-" + rightValue;

ValueJoiner<String, String, String> stringOuterJoiner = (leftValue, rightValue) ->
        "[StringOuterJoiner]" + leftValue + "<" + rightValue;

// inner join: 10초 이내에 양쪽 다 와야 결과가 나옴
KStream<String, String> joinedStream = leftStream.join(rightStream,
        stringJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)));

// outer join: 10초 이내에 한쪽만 와도 결과가 나옴 (없는 쪽은 null)
KStream<String, String> outerJoinedStream = leftStream.outerJoin(rightStream,
        stringOuterJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)));
```

### 예시 1 — Inner Join (양쪽 다 옴, key 일치, 10초 이내)

**입력:**
```
leftTopic  : key=1, value=leftValue   (t=0초)
rightTopic : key=1, value=rightValue  (t=3초)
```

**결과 (joinedMsg 토픽):**
```
[StringJoiner]leftValue-rightValue
[StringOuterJoiner]leftValue<rightValue
```
→ inner join과 outer join 둘 다 결과가 나온다. key가 같고(`1`) 10초 이내에 둘 다 왔기 때문.

**실제 테스트 결과 (이 프로젝트에서 검증):**
```
kafka-console-producer --topic leftTopic  --property "parse.key=true" --property "key.separator=:"
> join:lllll

kafka-console-producer --topic rightTopic --property "parse.key=true" --property "key.separator=:"
> join:rrrrr

→ joinedMsg 조회 결과:
[StringJoiner]lllll-rrrrr
[StringOuterJoiner]lllll<rrrrr
```

### 예시 2 — 한쪽만 옴 (outer join만 결과 나옴)

**입력:**
```
leftTopic  : key=l, value=1212   (t=0초)
rightTopic : (아무것도 안 옴, 또는 다른 key로 옴)
```

**결과:**
```
(inner join 결과 없음 — joinedMsg에 [StringJoiner]... 안 찍힘)
[StringOuterJoiner]1212<null
```
→ inner join은 짝이 없으면 아예 결과를 안 낸다. outer join은 없는 쪽을 `null`로 채워서 결과를 낸다.

**실제 테스트 결과:**
```
leftTopic key=l  value=1212  → 매칭 안 됨(rightTopic에 key=l 없음)
결과: [StringOuterJoiner]1212<null
```

### 예시 3 — key가 다르면 매칭 안 됨

**입력:**
```
leftTopic  : key=l, value=1212
rightTopic : key=r, value=212121
```

**결과:**
```
[StringOuterJoiner]1212<null    (leftTopic의 key=l 쪽)
[StringOuterJoiner]null<212121  (rightTopic의 key=r 쪽)
```
→ `l`과 `r`은 다른 key라서 서로 매칭되지 않고, 각자 outer join으로 "짝 없음" 처리된다. **key는 "왼쪽/오른쪽 구분용 라벨"이 아니라 실제 매칭 기준**이라는 걸 보여주는 예시.

### 예시 4 — 10초를 넘기면 매칭 안 됨

**입력:**
```
leftTopic  : key=1, value=leftValue   (t=0초)
rightTopic : key=1, value=rightValue  (t=15초, 윈도우 10초 초과)
```

**결과:**
```
[StringOuterJoiner]leftValue<null   (leftTopic 쪽, 짝을 못 찾고 윈도우 닫힘)
[StringOuterJoiner]null<rightValue  (rightTopic 쪽, 마찬가지)
```
→ `JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10))`이라 10초가 지나면 그 즉시 윈도우가 닫힌다(`NoGrace` = 여유시간 없음). inner join 결과는 안 나온다.

---

## 2. KTable-KTable Join (테이블 조인) — `AdEvaluationService.java`

테이블 대 테이블 조인은 **시간창이 필요 없다.** KTable은 "각 key별 최신 상태"를 계속 들고 있는 구조라서, 조인 시점에 양쪽 다 최신값이 있으면 무조건 결과가 나온다.

```java
KTable<String, WatchingAdLog> adTable = sb.stream("adLog", Consumed.with(Serdes.String(), watchingAdLogSerde))
        .selectKey((k,v) -> v.getUserId() + "_" + v.getProductId())
        .filter((k,v)-> Integer.parseInt(v.getWatchingTime()) > 10)  // 광고 시청시간 10초 초과만
        .toTable(...);

KTable<String, PurchaseLogOneProduct> purchaseLogOneProductKTable = sb.stream("purchaseLogOneProduct", ...)
        .selectKey((k,v)-> v.getUserId() + "_" + v.getProductId())
        .toTable(...);

adTable.join(purchaseLogOneProductKTable, tableStreamJoiner)
        .toStream().to("AdEvaluationComplete", ...);
```

### 예시 — 광고 시청 + 구매 이력 조인

**입력:**
```
adLog       : {"userId":"uid-0001","productId":"pg-0001","adId":"ad-101","watchingTime":"30", ...}
             → key로 변환: "uid-0001_pg-0001" (watchingTime=30 > 10 이라 통과)

purchaseLogOneProduct : {"userId":"uid-0001","productId":"pg-0001","price":"12000", ...}
             → key로 변환: "uid-0001_pg-0001" (adLog와 같은 key)
```

**결과 (AdEvaluationComplete 토픽):**
```json
{
  "userId": "uid-0001",
  "adId": "ad-101",
  "orderId": "od-...",
  "productInfo": {"productId": "pg-0001", "price": "12000"}
}
```
→ `selectKey`로 두 스트림 모두 **"userId_productId"** 형태로 key를 맞춰놨기 때문에 매칭된다. `watchingTime`이 10 이하였다면 애초에 `adTable`에 안 들어가서(`.filter()`) 조인 자체가 안 일어난다.

**차이점 요약:**
| 구분 | KStream-KStream (`StreamService`) | KTable-KTable (`AdEvaluationService`) |
|---|---|---|
| 시간창(JoinWindows) | 필수 | 불필요 |
| "짝을 놓치는" 경우 | 있음 (윈도우 밖이면 매칭 안 됨) | 없음 (양쪽 다 최신 상태가 있으면 항상 매칭) |
| 용도 | "특정 시점 근처에 발생한 두 이벤트"를 엮을 때 | "현재 상태(마스터 데이터)"끼리 항상 최신값으로 엮을 때 |

---

## 참고: 결과가 바로 안 뜨는 이유 / 중복되는 이유

- **지연**: Kafka Streams는 결과를 내부 캐시에 모았다가 `commit.interval.ms`(기본 30초) 주기로 다운스트림에 flush한다. 조인 자체는 즉시 일어나도 실제 토픽에 써지기까지 최대 30초 걸릴 수 있다.
- **중복**: 기본 처리 방식이 at-least-once라서, 앱이 재시작되면 커밋 안 된 레코드를 다시 처리해 같은 조인 결과가 여러 번 나올 수 있다. 데이터 유실이 아니라 중복이며, `StreamsConfig.PROCESSING_GUARANTEE_CONFIG=exactly_once_v2`로 없앨 수 있다(단, 성능 트레이드오프 있음).

## 관련 테스트 명령어

CLI로 직접 재현해보는 명령어는 [kafka-console-consumer-notes.md](./kafka-console-consumer-notes.md)의 "StreamService 데모 테스트 커맨드" 섹션 참고.
