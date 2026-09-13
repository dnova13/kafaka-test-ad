# 트러블슈팅 기록

## 1. Lombok + JDK 21 `NoSuchFieldError`

### 증상

```
java: java.lang.NoSuchFieldError: Class com.sun.tools.javac.tree.JCTree$JCImport does not have member field 'com.sun.tools.javac.tree.JCTree qualid'
```

### 원인

- 로컬 JDK가 21 (`java -version` → `21.0.8`)인데, `pom.xml`이 `spring-boot-starter-parent 3.0.1`을 상속하면서 Lombok 버전이 **1.18.24**로 고정됨.
- Lombok은 컴파일 시점에 `javac` 내부 클래스를 리플렉션으로 직접 건드리는데, 1.18.24는 JDK 21의 `javac` 내부 구조(`JCTree$JCImport`)를 아직 모름 → 필드를 못 찾아서 에러.
- Lombok이 JDK 21을 정식 지원한 건 1.18.30 이상부터.

### 조치

`pom.xml`에 `lombok.version` 프로퍼티를 명시적으로 올려서 parent가 관리하는 기본 버전을 덮어씀.

```xml
<properties>
    <java.version>17</java.version>
    <lombok.version>1.18.34</lombok.version>
</properties>
```

부수적으로 `lombok` 의존성이 중복 선언되어 있던 것(＜optional＞true 버전 + 버전 없는 버전 두 개)도 하나로 정리.

### 확인

```bash
mvn dependency:tree -Dincludes=org.projectlombok:lombok
# org.projectlombok:lombok:jar:1.18.34:compile
```

---

## 2. `Unsupported class file major version 65`

### 증상

```
Caused by: java.lang.IllegalArgumentException: Unsupported class file major version 65
    at org.springframework.asm.ClassReader.<init>
Caused by: java.io.IOException: ASM ClassReader failed to parse class file
Caused by: org.springframework.beans.factory.BeanDefinitionStoreException: Failed to read candidate component class
```

### 원인

- major version 65 = JDK 21로 컴파일된 클래스 파일.
- 이 프로젝트가 물고 있는 `spring-boot-starter-parent 3.0.1`(spring-core 6.0.3)은 2023년 1월 출시로, 2023년 9월에 나온 JDK 21보다 먼저 나온 버전이라 ASM이 21 클래스 포맷을 파싱하지 못함.
- 컴포넌트 스캔(`ClassPathScanningCandidateComponentProvider`)이 `.class` 파일을 읽다가 실패 → 앱 기동 자체가 안 됨.

### 조치

Spring Boot 버전을 올리는 대신, 컴파일 타깃을 JDK 17로 낮춤 (런타임 JDK는 그대로 21 사용, 하위 호환이라 문제없음).

```xml
<properties>
    <java.version>17</java.version>
</properties>
```

### 확인

```bash
mvn clean package -DskipTests
# target/classes/.../KafkaProducerApplication.class 의 major version이 65 -> 61(JDK17)로 변경됨 확인
```

---

## 3. Kafka Streams `INCOMPLETE_SOURCE_TOPIC_METADATA` / `MissingSourceTopicException`

### 증상

앱은 정상 기동(Tomcat 8080, `Started KafkaProducerApplication`)하지만, Kafka Streams 스레드만 아래 에러로 죽음.

```
o.a.k.s.p.internals.StreamThread : stream-thread [...] Received error code INCOMPLETE_SOURCE_TOPIC_METADATA
org.apache.kafka.streams.errors.MissingSourceTopicException: One or more source topics were missing during rebalance
... exception handler opted to SHUTDOWN_CLIENT ...
```

### 원인

- `AdEvaluationService`가 `sb.stream("adLog", ...)`, `sb.stream("purchaseLog", ...)`, `sb.stream("purchaseLogOneProduct", ...)`로 구독하는 소스 토픽 3개가 브로커에 아예 존재하지 않았음.
- 이 프로젝트는 로컬 3-브로커 클러스터(`pr4-code/kafka-docker-local/3_cluster-zookeeper`, `localhost:9092/9093/9094`)를 새로 띄운 상태라 아직 아무도 토픽을 만들거나 메시지를 produce한 적이 없었음.
- 브로커의 `auto.create.topics.enable`은 켜져 있지만(테스트 토픽으로 produce해서 자동 생성되는 것 확인), 자동 생성은 **produce 시점**에만 트리거되고 Kafka Streams가 시작 시점에 하는 **구독(consume)만으로는 트리거되지 않음** → 앱을 처음 켤 때는 무조건 실패.
- 참고로 이 코드베이스에는 애초에 `NewTopic`/`KafkaAdmin` 기반의 토픽 자동 생성 로직이 전혀 없었음(직접 확인, grep 결과 0건).

### 조치

`KafkaConfig`에 `KafkaAdmin` 빈과 `NewTopic` 빈 4개(`adLog`, `purchaseLog`, `purchaseLogOneProduct`, `AdEvaluationComplete`)를 추가해서 앱 기동 시 자동으로 토픽이 생성되도록 함. 파티션 수는 join 대상 토픽 간 co-partitioning을 맞추기 위해 전부 3으로 통일. 수동으로 만들고 싶을 때를 대비한 CLI 명령은 [kafka-topic-setup.md](./kafka-topic-setup.md) 참고.
