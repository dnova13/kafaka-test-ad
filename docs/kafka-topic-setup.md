# 카프카 토픽 수동 생성 가이드

`KafkaConfig`에 `NewTopic` 빈을 등록해두면 앱 기동 시 `KafkaAdmin`이 자동으로 토픽을 생성해준다.
다만 아래처럼 직접 CLI로 생성해야 할 상황(로컬 디버깅, 파티션 수를 앱 코드와 다르게 잡고 싶을 때 등)을 위해 수동 생성 방법을 정리한다.

## 필요한 토픽

`AdEvaluationService`의 스트림 토폴로지가 사용하는 토픽 4개.

| 토픽명 | 용도 |
|---|---|
| `adLog` | 광고 시청 로그 (KTable 소스) |
| `purchaseLog` | 구매 로그 (KStream 소스) |
| `purchaseLogOneProduct` | 상품 1개 단위로 분리된 구매 로그 (produce + 재소비) |
| `AdEvaluationComplete` | 광고 효과 평가 완료 결과 (최종 sink) |

## 사전 조건

`pr4-code/kafka-docker-local/3_cluster-zookeeper` 구성의 3-브로커 클러스터가 떠 있어야 한다.

```bash
docker ps --format "table {{.Names}}\t{{.Ports}}"
# zk-cluster-kafka-1  0.0.0.0:9092->9092/tcp
# zk-cluster-kafka-2  0.0.0.0:9093->9092/tcp
# zk-cluster-kafka-3  0.0.0.0:9094->9092/tcp
```

## 토픽 생성 명령어

컨테이너 내부의 `kafka-topics` CLI를 사용한다. `--replication-factor 3`은 브로커 3대가 모두 살아있어야 성공하며,
`KafkaConfig`의 `MIN_IN_SYNC_REPLICAS_CONFIG=2`, `NUM_STANDBY_REPLICAS_CONFIG=1` 설정과 맞춘 값이다.

```bash
docker exec -it zk-cluster-kafka-1 kafka-topics \
  --create --topic adLog \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 3

docker exec -it zk-cluster-kafka-1 kafka-topics \
  --create --topic purchaseLog \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 3

docker exec -it zk-cluster-kafka-1 kafka-topics \
  --create --topic purchaseLogOneProduct \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 3

docker exec -it zk-cluster-kafka-1 kafka-topics \
  --create --topic AdEvaluationComplete \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 3
```

호스트에 카프카 CLI가 설치되어 있다면(`brew install kafka` 등) 컨테이너 진입 없이 바로 실행 가능하다.

```bash
kafka-topics --create --topic adLog \
  --bootstrap-server localhost:9092,localhost:9093,localhost:9094 \
  --partitions 3 --replication-factor 3
```

## 생성 확인

```bash
docker exec -it zk-cluster-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

## 주의: 파티션 수를 반드시 동일하게

`purchaseLog`/`purchaseLogOneProduct`/`adLog`는 Kafka Streams에서 join되기 때문에 co-partitioning(파티션 수 동일)이 필요하다.
수동으로 만들 때 파티션 수를 다르게 주면 `TopologyException`(co-partitioning 에러)이 새로 발생하니 4개 토픽 모두 같은 파티션 수로 생성한다.

## 삭제가 필요할 때

```bash
docker exec -it zk-cluster-kafka-1 kafka-topics --delete --topic adLog --bootstrap-server localhost:9092
```
