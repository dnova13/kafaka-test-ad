# kafka-console-consumer 사용법 정리

## 강의 원본 커맨드 (EC2 3대 직접 접속)

```bash
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 13.125.129.151:9092, 52.78.82.145:9092, 13.125.110.158:9092 \
  --topic fastcampus \
  --from-beginning
```



강의에서는 카프카를 EC2 인스턴스 3대에 직접 설치했기 때문에, 로컬 PC에 내려받은 카프카 배포판(`~/kafka`)의 CLI 스크립트로 원격 브로커에 접속하는 방식이다.

## 로컬 도커용 커맨드

우리 환경은 카프카를 EC2에 설치한 게 아니라 `zk-cluster-kafka-1/2/3` 도커 컨테이너 3개로 띄워서 쓴다. `docker exec`로 브로커 컨테이너 "안"에 들어가서 실행하는 방식은 **신뢰할 수 없다** — kafka-1은 우연히 되고 kafka-2/3는 자기 자신조차 실패한다(이유는 아래 "왜 docker exec는 안 되는지" 참고). 대신 **같은 도커 네트워크에 별도의 임시 컨테이너를 붙여서** 실행하는 게 정답이다. 이게 강의에서 강사 노트북(EC2 3대 중 어디에도 속하지 않는 제3의 위치)이 하던 역할과 정확히 대응된다.

```bash
docker run --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1 \
  kafka-console-consumer --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  --topic defaultTopic \
  --from-beginning
```

(`defaultTopic`은 `Producer.pub()`/`pubString()`이 실제로 보내는 토픽 — `ProducerController`의 `/message`, `/smp-msg` 엔드포인트로 보낸 메시지를 확인하려면 이 토픽을 봐야 한다.)

- `--network 3_cluster-zookeeper_default`: `docker-compose.yml`이 자동으로 만든 네트워크 이름(`docker inspect <컨테이너> --format '{{json .NetworkSettings.Networks}}'`로 확인 가능). 이 네트워크에 붙여야 컨테이너 이름(`kafka-1` 등)으로 서로를 찾을 수 있다.
- `kafka-1:29092,kafka-2:29092,kafka-3:29092`: 브로커들의 **INTERNAL 리스너** 주소(포트 통일 이유는 `kafka-docker-local/3_cluster-zookeeper/docker-compose.yml`의 `KAFKA_LISTENERS` 주석 참고). 어느 파티션의 리더든, 컨슈머 그룹 코디네이터가 어디든 상관없이 항상 접속 가능하다.
- `--rm`: 명령 끝나면 이 임시 컨테이너는 자동 삭제되고, 브로커 3대 상태에는 아무 영향 없음.

### 왜 `docker exec`는 안 되는지

호스트 포트(`localhost:9092/9093/9094`)는 **호스트에서 접속할 때만** 유효한 주소다. 브로커 컨테이너 "안"에서 카프카 클라이언트가 메타데이터를 받으면 자기 자신을 포함한 모든 브로커의 EXTERNAL 광고 주소(`localhost:909X`)를 그대로 믿고 접속을 시도하는데:
- `kafka-1`은 광고 포트(9092)와 컨테이너 내부 실제 리스닝 포트(9092)가 **우연히 같아서** 자기 자신에게만 어쩌다 성공한다.
- `kafka-2`/`kafka-3`는 광고 포트(9093/9094)가 내부 실제 포트(9092)와 **달라서**, 자기 자신을 향한 접속마저 실패한다.

그래서 어떤 토픽·파티션이든 리더나 컨슈머 그룹 코디네이터가 kafka-2/3로 잡히는 순간 `docker exec`로는 절대 안 된다. 위의 "정답" 커맨드(별도 컨테이너 + INTERNAL 주소)를 쓸 것.

## 호스트에 카프카 CLI 직접 설치해서 쓰기

`brew install kafka`로 호스트(맥)에 CLI만 깔아서 쓰는 방식. 브로커는 그대로 도커가 담당하고, 이 CLI는 조회용 클라이언트 역할만 한다. 컨테이너 진입/임시 컨테이너 실행 없이 포트매핑된 주소로 바로 접속되고, EXTERNAL 리스너를 정상적으로 쓰는 것이라 `docker exec`의 리더/코디네이터 문제도 없다.

### 설치

```bash
brew install kafka
```

설치되면 `kafka-console-consumer`, `kafka-console-producer`, `kafka-topics`, `kafka-consumer-groups`, `kafka-get-offsets` 등이 바로 PATH에 잡힌다(`.sh` 확장자 없이 호출).

### 자주 쓰는 명령어

토픽 목록 확인:
```bash
kafka-topics --list --bootstrap-server localhost:9092,localhost:9093,localhost:9094

docker run --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1 \
kafka-topics --list --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092
```

토픽 상세(파티션/리더/레플리카) 확인:
```bash
kafka-topics --describe --topic defaultTopic \
  --bootstrap-server localhost:9092,localhost:9093,localhost:9094

docker run --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1 \
kafka-topics --describe --topic defaultTopic --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092
```

토픽 생성:
```bash
kafka-topics --create --topic defaultTopic \
  --bootstrap-server localhost:9092,localhost:9093,localhost:9094 \
  --partitions 3 --replication-factor 3

docker run --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1 \
kafka-topics --create --topic defaultTopic --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --partitions 3 --replication-factor 3
```

메시지 읽기 (컨슈머) — 백엔드 `/message`, `/smp-msg`로 보낸 메시지 확인용:
```bash
kafka-console-consumer --bootstrap-server localhost:9092,localhost:9093,localhost:9094 \
  --topic defaultTopic \
  --from-beginning
```

메시지 보내기 (프로듀서, 실행하면 프롬프트가 뜨고 한 줄씩 입력 후 엔터):
```bash
kafka-console-producer --bootstrap-server localhost:9092,localhost:9093,localhost:9094 \
  --topic defaultTopic
  
docker run -it --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1 kafka-console-producer \
--bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \ 
--topic defaultTop
```




토픽의 최소/최대 오프셋(=메시지 존재 여부) 확인:
```bash
kafka-get-offsets --bootstrap-server localhost:9092,localhost:9093,localhost:9094 \
  --topic defaultTopic

docker run --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1 \
kafka-get-offsets --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --topic defaultTopic
```

토픽 삭제:
```bash
kafka-topics --delete --topic defaultTopic \
  --bootstrap-server localhost:9092,localhost:9093,localhost:9094

docker run --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1 \
kafka-topics --delete --topic defaultTopic --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092
```

> 모든 명령에서 `--bootstrap-server`는 3개를 다 적든 1개만 적든 결과는 같다(부트스트랩 성공 후 메타데이터로 전체 클러스터를 알게 되므로). 3개를 다 적어두면 그중 하나가 죽어 있어도 접속 실패하지 않는 안전장치가 된다.

## 커맨드 상세 설명

| 요소 | 설명 |
|---|---|
| `docker run --rm --network 3_cluster-zookeeper_default confluentinc/cp-kafka:7.6.1` | 브로커 3대 중 어디에도 속하지 않는 임시 컨테이너 하나를 같은 도커 네트워크에 새로 띄워서 그 안에서 명령을 실행한다. 끝나면 `--rm`으로 자동 삭제됨. |
| `kafka-console-consumer` | 컨플루언트 카프카 이미지에 기본 내장된 CLI 도구. 원래 이름은 `kafka-console-consumer.sh`인데, 도커 이미지 안에서는 `.sh` 없이 PATH에 심볼릭 링크 되어 있어 바로 호출 가능하다. |
| `--bootstrap-server` | 클라이언트가 클러스터에 처음 접속할 때 사용하는 진입점 주소. 하나만 넘겨도 그 브로커가 전체 클러스터의 메타데이터(다른 브로커 목록, 파티션 리더 정보 등)를 응답해주기 때문에 3개를 다 나열할 필요는 없다. 강의에서 3개를 다 적은 건 그중 하나가 죽어 있어도 접속에 실패하지 않도록 하는 관용적인 방식(고가용성 대비)이다. |
| `--topic fastcampus` | 구독할 토픽 이름. 이 토픽에 쌓인 메시지를 읽어온다. |
| `--from-beginning` | 해당 토픽/파티션의 가장 오래된(맨 앞) 오프셋부터 읽는다. 이 옵션이 없으면 컨슈머가 실행된 시점 이후에 새로 들어오는 메시지만 보인다. |

## StreamService 데모 테스트 커맨드 (필터 / 조인)

`StreamService.java`의 두 데모(필터링, KStream-KStream 조인)를 실제로 테스트할 때 쓴 명령어 모음.

### 필터 데모 (fastcampus → freeClassList)

`fastcampus`에 아무 메시지나 보내면 전부 콘솔에 찍히고(`print()`), 그중 `"freeClass"` 문자열이 포함된 것만 `freeClassList` 토픽으로 걸러져서 들어간다.

메시지 보내기:
```bash
kafka-console-producer --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic fastcampus
```
```
오늘 freeClass 특강 안내
그냥 일반 공지사항
```

필터링 결과 확인 (`"freeClass"` 포함된 것만 나와야 정상):
```bash
kafka-console-consumer --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic freeClassList --from-beginning
```

> `myStream.print(...)`는 `myStream.filter(...).to(...)`와 **별개 분기**라서, 필터 결과와 상관없이 `fastcampus`로 들어오는 건 IntelliJ 콘솔에 전부 찍힌다(필터링 안 되는 것처럼 보이는 게 정상). key 없이 보내면 `print()` 출력의 key 자리에 항상 `null`이 찍힌다.

### 조인 데모 (leftTopic + rightTopic → joinedMsg)

같은 key로 leftTopic, rightTopic에 **10초 이내**로 메시지를 보내야 조인된다(`JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10))`). key가 다르면 매칭 안 되고, key가 아예 없으면(null) join 단계에서 조용히 버려진다.

leftTopic에 key:value 형태로 보내기 (`parse.key=true` 필수):
```bash
kafka-console-producer --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic leftTopic --property "parse.key=true" --property "key.separator=:"
```
```
1:leftValue
```

rightTopic도 같은 key로, 위 leftTopic 입력 후 **10초 이내에** 이어서:
```bash
kafka-console-producer --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic rightTopic --property "parse.key=true" --property "key.separator=:"
```
```
1:rightValue
```

조인 결과 확인:
```bash
kafka-console-consumer --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic joinedMsg --from-beginning
```
정상 매칭되면 이런 식으로 나온다:
```
[StringJoiner]leftValue-rightValue
[StringOuterJoiner]leftValue<rightValue
```

> - `--property`는 최신 버전에서 deprecated 경고가 뜨지만(`Use --reader-property instead`) 여전히 동작한다. 새 문법을 쓰려면 `--reader-property`로 바꾸면 된다.
> - 결과가 바로 안 뜨고 시간이 좀 걸릴 수 있다 — Kafka Streams의 record cache가 `commit.interval.ms`(기본 30초) 주기로만 다운스트림에 flush하기 때문. 즉시 보고 싶으면 `KafkaConfig`의 스트림 설정에 `CACHE_MAX_BYTES_BUFFERING_CONFIG=0` 또는 `COMMIT_INTERVAL_MS_CONFIG`를 줄이면 된다.
> - 같은 결과가 여러 번 중복 출력될 수 있다 — Kafka Streams 기본값이 at-least-once 처리라서, 앱이 재시작되면 커밋 안 된 레코드를 다시 처리해 같은 조인 결과를 또 낼 수 있다. 데이터 유실이 아니라 중복이며, `exactly_once_v2`로 바꾸면 없앨 수 있지만 성능 트레이드오프가 있다.

## 참고

- `zk-cluster-kafka-1`은 `localhost:9092`, `zk-cluster-kafka-2`는 `localhost:9093`, `zk-cluster-kafka-3`은 `localhost:9094`로 각각 호스트 포트에 매핑되어 있다.
- 콘솔 컨슈머는 종료 시 `Ctrl+C`.
- `fastcampus` 토픽은 현재(2026-09-12 기준) 메시지가 0건이라 실행해도 아무 출력 없이 대기만 하는 상태다.
