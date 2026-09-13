# kafka-ui (Provectus) 가이드

터미널 CLI 대신 브라우저에서 카프카 토픽/메시지/컨슈머 그룹을 조회·조작할 수 있는 웹 GUI. `kafka-ui`는 카프카 데이터를 자체 저장하지 않고, 브로커에 붙어서 실시간으로 조회/조작만 하는 얇은 클라이언트다(phpMyAdmin ↔ MySQL 관계와 동일).

## 세팅 방식

`pr4-code/kafka-docker-local/3_cluster-zookeeper/docker-compose.yml`에 서비스로 등록되어 있다.

```yaml
kafka-ui:
  image: provectuslabs/kafka-ui:latest
  container_name: zk-cluster-kafka-ui
  ports:
    - "8090:8080"
  environment:
    KAFKA_CLUSTERS_0_NAME: local
    KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-1:29092,kafka-2:29092,kafka-3:29092
  depends_on:
    - kafka-1
    - kafka-2
    - kafka-3
```

- **`kafka-1:29092,...`(INTERNAL 리스너)를 쓰는 이유**: kafka-ui도 도커 컨테이너라서, 같은 네트워크(`3_cluster-zookeeper_default`) 안의 다른 컨테이너(브로커)를 컨테이너 이름으로 찾아갈 수 있다. `localhost:909X`(EXTERNAL, 호스트용)를 쓰면 지난번 겪었던 것과 똑같은 이유로 연결이 안 된다.
- **포트 8090**: 로컬 맥에서 이미 Postgres(5432), 앱(8080) 등을 쓰고 있어서 안 겹치는 8090으로 매핑.

### 실행 / 중지

```bash
cd "pr4-code/kafka-docker-local/3_cluster-zookeeper"

# 기존 브로커/주키퍼는 그대로 두고 kafka-ui만 새로 띄우기
docker compose up -d kafka-ui

# 중지 (브로커는 안 건드림)
docker compose stop kafka-ui

# 완전 삭제
docker compose rm -f kafka-ui
```

### 접속

브라우저에서 **http://localhost:8090** 접속. 로그인 없이 바로 대시보드가 뜬다(별도 인증 설정 안 함).

### 정상 연결 확인

```bash
curl http://localhost:8090/api/clusters
```
`"status":"online"`, `"brokerCount":3`이 보이면 정상.

---

## 사용 방법

### 1. 토픽 목록/상세 보기
좌측 메뉴 **Topics** 클릭 → 토픽 이름 클릭하면:
- **Overview** 탭: 파티션 수, 복제본, 메시지 수, 크기
- **Messages** 탭: 실제 메시지 내용 조회 (offset, timestamp, key, value 컬럼) — `kafka-console-consumer --from-beginning` 대체
- **Consumers** 탭: 이 토픽을 구독 중인 컨슈머 그룹과 lag
- **Settings** 탭: 토픽 설정값(`min.insync.replicas` 등) 확인

### 2. 메시지 직접 보내기 (Produce)
토픽 상세 페이지 우측 상단 **Produce Message** 버튼 클릭 →
- `Key` 입력창에 key 입력 (선택)
- `Value` 입력창에 JSON/문자열 값 입력
- **Produce Message** 클릭

→ `kafka-console-producer`로 타이핑하던 걸 그대로 대체. 우리 프로젝트의 `adLog`/`purchaseLog`에 JSON 보낼 때 이 화면에서 바로 테스트 가능.

### 3. 토픽 생성/삭제
**Topics** 목록 화면 우측 상단 **Add a Topic**으로 파티션 수/복제본 수 지정해서 생성. 토픽 상세 페이지의 점 세개 메뉴(⋮)에서 **Remove Topic**으로 삭제.

### 4. 컨슈머 그룹 확인
좌측 메뉴 **Consumers** → 그룹 이름(`foo`, `ad-eval-db-writer`, `lecture-6` 등) 클릭하면 그룹이 구독 중인 각 파티션의 `CURRENT-OFFSET` / `LOG-END-OFFSET` / `LAG`을 표로 확인 — `kafka-consumer-groups --describe` 대체.

### 5. 브로커 상태 확인
좌측 메뉴 **Brokers** → 브로커별 파티션 리더 개수, 디스크 사용량 등 확인.

---

## 치트 시트 — CLI ↔ kafka-ui 대응표

| 지금까지 쓰던 CLI 명령 | kafka-ui에서 |
|---|---|
| `kafka-topics --list` | Topics 메뉴 목록 |
| `kafka-topics --describe --topic X` | Topics → X 클릭 → Overview/Settings 탭 |
| `kafka-topics --create --topic X --partitions N --replication-factor N` | Topics → Add a Topic |
| `kafka-topics --delete --topic X` | Topics → X → ⋮ → Remove Topic |
| `kafka-console-consumer --topic X --from-beginning` | Topics → X → Messages 탭 |
| `kafka-console-producer --topic X` | Topics → X → Produce Message |
| `kafka-consumer-groups --describe --group G` | Consumers → G 클릭 |
| `kafka-get-offsets --topic X` | Topics → X → Overview 탭의 메시지 수/오프셋 정보 |

## 주의사항

- kafka-ui는 **조회/조작 도구일 뿐**, 데이터를 저장하지 않는다 — 컨테이너를 지워도 카프카 데이터엔 영향 없음.
- 브로커 3대(`zk-cluster-kafka-1/2/3`)가 떠 있어야 정상 작동한다. `docker ps`로 먼저 확인.
- 이 문서와 별개로, 애플리케이션 기능 테스트(폼으로 adLog/purchaseLog 보내고 결과를 DB에서 확인)는 `http://localhost:8080`의 자체 프론트 페이지를 이용한다 (kafka-ui는 범용 카프카 관리 도구, 그 프론트 페이지는 이 프로젝트 전용 기능).
