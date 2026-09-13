# KStream vs KTable 정리

## 한 줄 정의

- **KStream** = 카프카 토픽에 들어오는 메시지를 **"독립된 이벤트(사실)의 연속"**으로 취급. 같은 key가 여러 번 와도 전부 각각 별개의 레코드로 다룬다. → **INSERT** 의미론.
- **KTable** = 같은 토픽을 **"key별 현재 상태(최신값)"**로 취급. 같은 key로 새 메시지가 오면 이전 값을 **덮어쓴다**. → **UPSERT** 의미론.

**같은 원본 데이터라도 KStream으로 보느냐 KTable로 보느냐에 따라 의미가 완전히 달라진다.** 이게 이 문서의 핵심이다.

---

## 예시로 바로 이해하기

토픽에 이런 순서로 메시지 3개가 들어왔다고 하자 (key, value):

```
(1, "A")   t=0초
(1, "B")   t=1초   ← key 1이 다시 옴
(2, "C")   t=2초
```

### KStream으로 보면

3개 전부 **독립된 이벤트**다. "key=1인 이벤트가 두 번 발생했다"고 해석한다.

```java
KStream<String, String> stream = sb.stream("myTopic", ...);
stream.print(Printed.toSysOut());
```
```
1, A
1, B
2, C
```
3줄 다 찍힌다. 카운트를 세면 "key=1: 2번 발생, key=2: 1번 발생"이 된다.

### KTable로 보면

같은 데이터인데, **key=1의 "현재 값"이 A에서 B로 업데이트**된 것으로 해석한다. 최종 상태는:

```
key=1 → B   (A는 B로 덮어써져서 사라짐)
key=2 → C
```

```java
KTable<String, String> table = sb.table("myTopic", ...);
```
내부 상태 저장소(state store)를 조회하면 `{1: B, 2: C}` 두 개만 존재한다 — A는 "지나간 과거 값"이라 현재 상태 조회에는 안 나온다.

> 참고: `table.toStream().print(...)`로 다시 스트림으로 바꿔서 찍으면 변경 이력(changelog)이라 3줄이 그대로 다 보인다. "최신 값만 남는다"는 건 **상태 저장소(state store)를 조회할 때**(join, aggregation, interactive query) 드러나는 특성이지, 겉으로 흘러가는 메시지 자체가 줄어드는 게 아니다.

---

## 왜 이런 차이가 생기나 — "로그"와 "테이블"의 관계

카프카 토픽은 원래 **append-only 로그**(순서대로 쌓이기만 하는 기록)다. KStream은 이 로그를 있는 그대로 읽는 것이고, KTable은 "이 로그를 순서대로 재생하면서 key별로 최신 값을 계속 덮어쓴 결과"를 유지하는 것이다.

비유하면:
- **KStream** = 은행 거래 내역서(입금 500원, 출금 200원, 입금 1000원... 전부 개별 기록)
- **KTable** = 지금 통장 잔고 (거래 내역을 순서대로 다 반영한 **최종 결과 하나**)

거래 내역(KStream)을 순서대로 다 더하면 잔고(KTable)가 나온다 — 이게 Kafka Streams에서 말하는 **"Stream-Table Duality(스트림-테이블 이중성)"**다. 둘은 완전히 다른 게 아니라, **같은 데이터를 보는 두 가지 관점**이다.

---

## 서로 변환 가능하다

| 방향 | 방법 | 이 프로젝트 예시 |
|---|---|---|
| KStream → KTable | `.toTable()` | `KTableService.java`: `sb.stream("leftTopic", ...).toTable()` |
| 토픽 → 곧바로 KTable | `sb.table("topic", ...)` | (이 프로젝트엔 없지만 `.stream().toTable()`과 결과는 같음) |
| KTable → KStream | `.toStream()` | `AdEvaluationService.java`: `adTable.join(...).toStream().to("AdEvaluationComplete", ...)` |

즉 "이건 무조건 KStream", "저건 무조건 KTable"로 고정된 게 아니라, **같은 토픽을 상황에 맞게 어느 쪽으로 읽을지 코드에서 선택**하는 것이다.

---

## 실제로 왜 구분해서 쓰는가 — Join에서 드러나는 차이

이 프로젝트에 세 가지 조인이 있는데, 이 차이 때문에 코드가 달라진다. (자세한 예시는 [kafka-streams-join-guide.md](./kafka-streams-join-guide.md) 참고)

| | `StreamService` (KStream-KStream) | `KTableService` (KTable-KTable) | `AdEvaluationService` (KTable-KTable) |
|---|---|---|---|
| 데이터를 보는 관점 | "이 순간 발생한 이벤트끼리 엮기" | "현재 상태끼리 엮기" | "현재 상태끼리 엮기" |
| 시간창(JoinWindows) | **필수** — 이벤트는 스쳐 지나가므로 "언제까지 기다릴지" 정해야 함 | **불필요** — 상태는 계속 남아있어서 아무 때나 최신값끼리 매칭 | **불필요** |
| key가 같은 메시지가 여러 번 오면 | 매번 다 개별 매칭 시도 (중복 매칭 가능) | 이전 값은 덮어써지고 최신 값끼리만 매칭 | 동일 |

**왜 `AdEvaluationService`는 KTable을 썼나**: 광고 시청 이력과 구매 이력을 "특정 순간에 우연히 겹친 이벤트"로 보는 게 아니라, "이 유저의 이 상품에 대한 현재 상태(광고 봤음 + 구매함)"로 보고 싶기 때문이다. `StreamService`의 데모는 반대로 "10초 이내에 발생한 이벤트끼리만 엮는" 시나리오를 보여주려고 일부러 KStream-KStream을 쓴 것이다.

---

## 요약

- KStream = 이벤트 하나하나가 다 의미 있음 (거래 내역)
- KTable = key별 최신 상태만 의미 있음 (통장 잔고)
- 원본은 같은 토픽/로그이고, 어떻게 "읽을지"만 다르다 (`.stream()` vs `.table()`), 서로 변환도 가능하다 (`.toTable()` / `.toStream()`)
- 이 차이 때문에 join 방식(윈도우 필요 여부)이 완전히 달라진다
