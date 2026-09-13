# AdEvaluationService 테스트 가이드

`AdEvaluationService`(광고 시청 + 구매 이력을 조인해서 광고 효과를 판정하는 파이프라인)를 실제로 테스트하는 방법 정리. 강의에서는 EC2 3대(`172.31.20.112`, `172.31.9.182`, `172.31.7.201`)에서 터미널 4개를 띄워 테스트했고, 우리는 도커 환경으로 옮겨서 동일하게 재현한다.

전체 구조는 [AdEvaluationService 설명](#adevaluationservice-흐름-요약) 참고. 필요한 토픽: `adLog`, `purchaseLog`, `purchaseLogOneProduct`(중간 산출물, 자동 생성됨), `AdEvaluationComplete`(최종 결과) — 전부 `KafkaConfig.java`에 `NewTopic` 빈으로 이미 만들어져 있어서 따로 생성할 필요 없다.

---

## ⚠️ 가장 중요한 주의사항 — `purchaseLog`의 JSON 스키마

테스트 중 실수하기 제일 쉬운 부분이라 먼저 짚는다. `PurchaseLog.java`가 요구하는 정확한 형태는 이렇다.

**❌ 틀린 예시 (실제로 강의 테스트 중 한 번 이렇게 보냈다가 실패함):**
```json
{ "orderId": "od-0003", "userId": "uid-0007", "productId": ["pg-0017", "pg-0007"], "purchasedDt": "20230201070000", "price": 24000}
```
→ `productId`가 그냥 **문자열 배열**. `PurchaseLog.productInfo`는 `ArrayList<Map<String,String>>` 타입이라 이 구조를 못 읽는다(또는 값이 다 비어서 들어감). 실제로 이 스키마로 보냈을 때 최종 결과에 `productInfo`가 없거나 `orderId: null`로 깨져서 나왔다.

**✅ 맞는 예시:**
```json
{ "orderId": "od-0005", "userId": "uid-0005", "productInfo": [{"productId": "pg-0023", "price":"12000"}, {"productId":"pg-0022", "price":"13500"}], "purchasedDt": "20230201070000", "price": 24000}
```
→ 필드명은 `productInfo`(단수 `productId`가 아님), 그 안에 `{productId, price}` **객체** 배열. `PurchaseLog.java` 상단 주석에 있는 SAMPLE DATA와 정확히 같은 형태.

**`adLog` 스키마 (`WatchingAdLog.java` 기준, 문제 없음):**
```json
{"userId": "uid-0005", "productId": "pg-0022", "adId": "ad-101", "adType": "banner", "watchingTime": "30", "watchingDt": "20230201070000"}
```

---

## 강의 원본 커맨드 (EC2 3대, 터미널 4개)

### 터미널 1 — adLog 프로듀서 (172.31.20.112)
```bash
~/kafka/bin/kafka-topics.sh --create --zookeeper 172.31.20.112:2181,172.31.9.182:2181,172.31.7.201:2181 --replication-factor 3 --partitions 1 --topic adLog

~/kafka/bin/kafka-console-producer.sh --broker-list 172.31.20.112:9092,172.31.9.182:9092,172.31.7.201:9092 --topic adLog
```
```
{"userId": "uid-0005", "productId": "pg-0022", "adId": "ad-101", "adType": "banner", "watchingTime": "30", "watchingDt": "20230201070000"}
```

### 터미널 2 — purchaseLogOneProduct 확인용 컨슈머 (172.31.9.182)
```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server 172.31.20.112:9092,172.31.9.182:9092,172.31.7.201:9092 --topic purchaseLogOneProduct --from-beginning
```
→ `AdEvaluationService`가 `purchaseLog`를 상품 1개 단위로 쪼개서 이 토픽에 재발행한 결과가 자동으로 여기 찍힌다(직접 보낼 필요 없음, `foreach`가 알아서 produce).

### 터미널 3 — purchaseLog 프로듀서 (172.31.7.201)
```bash
~/kafka/bin/kafka-topics.sh --create --zookeeper 172.31.20.112:2181,172.31.9.182:2181,172.31.7.201:2181 --replication-factor 3 --partitions 1 --topic purchaseLog

~/kafka/bin/kafka-console-producer.sh --broker-list 172.31.20.112:9092,172.31.9.182:9092,172.31.7.201:9092 --topic purchaseLog
```
```
{ "orderId": "od-0005", "userId": "uid-0005", "productInfo": [{"productId": "pg-0023", "price":"12000"}, {"productId":"pg-0022", "price":"13500"}], "purchasedDt": "20230201070000", "price": 24000}
```

### 터미널 4 — 최종 결과 확인 (172.31.9.182)
```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server 172.31.20.112:9092,172.31.9.182:9092,172.31.7.201:9092 --topic AdEvaluationComplete --from-beginning
```

---

## 우리 환경(도커)에서 재현하는 법

터미널 4개 대신, 아래 4개 명령을 각각 다른 터미널 탭에서 실행하면 된다. `--network 3_cluster-zookeeper_default`로 붙는 이유와 `kafka-1:29092,kafka-2:29092,kafka-3:29092`(INTERNAL 리스너)를 쓰는 이유는 [kafka-console-consumer-notes.md](./kafka-console-consumer-notes.md) 참고. 호스트에 `brew install kafka`로 CLI를 깔았다면 `localhost:9092,localhost:9093,localhost:9094`로 바꿔써도 동일하다.

### 1) adLog 프로듀서
```bash
docker run -it --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1 kafka-console-producer --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --topic adLog
```
```
{"userId": "uid-0005", "productId": "pg-0022", "adId": "ad-101", "adType": "banner", "watchingTime": "30", "watchingDt": "20230201070000"}
```

### 2) purchaseLogOneProduct 확인용 컨슈머 (미리 켜두고 지켜보기)
```bash
docker run --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1 kafka-console-consumer --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --topic purchaseLogOneProduct --from-beginning
```

### 3) purchaseLog 프로듀서 (정확한 스키마로!)
```bash
docker run -it --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1 kafka-console-producer --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --topic purchaseLog
```
```
{ "orderId": "od-0005", "userId": "uid-0005", "productInfo": [{"productId": "pg-0023", "price":"12000"}, {"productId":"pg-0022", "price":"13500"}], "purchasedDt": "20230201070000", "price": 24000}
```

### 4) 최종 결과 확인
```bash
docker run --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1 kafka-console-consumer --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --topic AdEvaluationComplete --from-beginning
```

---

## 실제 결과값 (정상 케이스)

위 순서대로 보내면 이런 흐름으로 결과가 나온다.

**2번 터미널 (`purchaseLogOneProduct`) — `purchaseLog`가 상품별로 쪼개져서 나옴:**
```
{"orderId":"od-0005","userId":"uid-0005","productId":"pg-0023","purchasedDt":"20230201070000","price":"12000"}
{"orderId":"od-0005","userId":"uid-0005","productId":"pg-0022","purchasedDt":"20230201070000","price":"13500"}
```

**4번 터미널 (`AdEvaluationComplete`) — 최종 조인 결과:**
```json
{"adId":"ad-101","userId":"uid-0005","orderId":"od-0005","productInfo":{"productId":"pg-0022","price":"13500"}}
```

**왜 `pg-0023`은 결과에 안 나오고 `pg-0022`만 나왔나?**

`adLog`에서 광고 시청한 상품은 `pg-0022` 하나뿐이었다(`productId: "pg-0022"`). `AdEvaluationService`는 `userId + "_" + productId`를 key로 삼아 조인하기 때문에:
- `uid-0005_pg-0022` → adLog에도 있고 purchaseLog에도 있음 → **매칭됨** → 결과 나옴
- `uid-0005_pg-0023` → purchaseLog엔 있지만 adLog엔 없음(광고를 안 봄) → **매칭 안 됨** → 결과 없음

즉 "광고를 본 상품만 구매로 이어졌는지"를 정확히 걸러내는 게 의도된 동작이다.

---

## 실패 케이스 (스키마 틀렸을 때 — 참고용)

`productInfo` 대신 `productId`를 문자열 배열로 잘못 보내면:
```json
{ "orderId": "od-0003", "userId": "uid-0007", "productId": ["pg-0017", "pg-0007"], "purchasedDt": "20230201070000", "price": 24000}
```
`purchaseLogOneProduct`까지는 만들어지지만(`AdEvaluationService`의 `foreach`가 `productInfo`를 순회하는데 이 필드가 없으니 실제로는 비거나 예외 처리 경로를 탐), 최종 `AdEvaluationComplete` 결과가 깨져서 나온다:
```json
{"adId":"ad-101","userId":"uid-0007"}
{"adId":"ad-102","userId":"uid-0007","orderId":null}
```
`orderId`가 `null`이거나 `productInfo`가 아예 빠진 걸로 봐서, 조인은 됐지만 데이터 자체가 불완전한 상태로 만들어졌다는 뜻 — **테스트 시 JSON 스키마부터 다시 확인**하는 게 우선이다.

---

## AdEvaluationService 흐름 요약

자세한 코드 설명은 `AdEvaluationService.java`의 상세 주석 및 이전 대화 참고. 한 줄 요약:

```
adLog (광고 시청) ──┐
                    ├─ userId_productId 키로 join ─→ AdEvaluationComplete (광고 효과 결과)
purchaseLog (구매) ─┘   (purchaseLog는 중간에 purchaseLogOneProduct로 상품 단위 재발행 거침)
```
