package com.example.kafkaExercise.config;

import com.example.kafkaExercise.util.PurchaseLogOneProductSerializer;
import com.example.kafkaExercise.util.PurchaseLogSerializer;
import com.example.kafkaExercise.util.WatchingAdLogSerializer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
@EnableKafka
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Claude 추가: spring.kafka.bootstrap-servers가 아닌 kafka.bootstrap-servers를 쓰고 있어
    // KafkaAdmin이 어떤 브로커에 접속할지 명시적으로 지정함
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }


    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration myKStreamConfig() {
        Map<String, Object> myKStreamConfig = new HashMap<>();
        myKStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "lecture-6");
        myKStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        myKStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        myKStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        myKStreamConfig.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        myKStreamConfig.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), 2);
        myKStreamConfig.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        // Claude 추가: 위에서 min.insync.replicas=2를 요구하는데 정작 내부 토픽(join/toTable용
        // changelog, repartition 등)의 복제본 개수를 안 정해주면 기본값 1로 생성돼서
        // "복제본 1개인데 2개 동기화를 요구"하는 모순 상태가 되어 NOT_ENOUGH_REPLICAS로
        // 영원히 재시도만 하게 됨. 브로커 3대에 맞춰 3으로 명시.
        myKStreamConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        return new KafkaStreamsConfiguration(myKStreamConfig);
    }


    @Bean
    public KafkaTemplate<String, Object> KafkaTemplateForGeneral() {
        return new KafkaTemplate<String, Object>(ProducerFactory());
    }

    @Bean
    public ProducerFactory<String, Object> ProducerFactory() {
        Map<String, Object> myConfig = new HashMap<>();

        myConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        myConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        myConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PurchaseLogOneProductSerializer.class);

        return new DefaultKafkaProducerFactory<>(myConfig);
    }

    @Bean
    public KafkaTemplate<String, Object> KafkaTemplateForWatchingAdLog() {
        return new KafkaTemplate<String, Object>(ProducerFactoryForWatchingAdLog());
    }
    @Bean
    public ProducerFactory<String, Object> ProducerFactoryForWatchingAdLog() {
        Map<String, Object> myConfig = new HashMap<>();

        myConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        myConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        myConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, WatchingAdLogSerializer.class);

        return new DefaultKafkaProducerFactory<>(myConfig);
    }

    // Claude 추가: 순수 문자열 메시지 테스트용 (value serializer가 StringSerializer)
    @Bean
    public KafkaTemplate<String, String> KafkaTemplateForString() {
        return new KafkaTemplate<>(ProducerFactoryForString());
    }

    @Bean
    public ProducerFactory<String, String> ProducerFactoryForString() {
        Map<String, Object> myConfig = new HashMap<>();

        myConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        myConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        myConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(myConfig);
    }

    @Bean
    public KafkaTemplate<String, Object> KafkaTemplateForPurchaseLog() {
        return new KafkaTemplate<String, Object>(ProducerFactoryForPurchaseLog());
    }
    @Bean
    public ProducerFactory<String, Object> ProducerFactoryForPurchaseLog() {
        Map<String, Object> myConfig = new HashMap<>();

        myConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        myConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        myConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PurchaseLogSerializer.class);

        return new DefaultKafkaProducerFactory<>(myConfig);
    }


    // Claude 추가: KafkaAdmin이 앱 기동 시 아래 NewTopic 빈들을 보고 없는 토픽만 자동으로 생성함
    // (원래 코드엔 토픽 자동 생성 로직이 없어서 Kafka Streams가 MissingSourceTopicException으로 죽던 문제 해결용)
    // join 대상 토픽들은 co-partitioning을 위해 파티션 수를 동일하게 맞춰야 함
    @Bean
    public NewTopic adLogTopic() {
        return TopicBuilder.name("adLog").partitions(3).replicas(3).build();
    }

    @Bean
    public NewTopic purchaseLogTopic() {
        return TopicBuilder.name("purchaseLog").partitions(3).replicas(3).build();
    }

    @Bean
    public NewTopic purchaseLogOneProductTopic() {
        return TopicBuilder.name("purchaseLogOneProduct").partitions(3).replicas(3).build();
    }

    @Bean
    public NewTopic adEvaluationCompleteTopic() {
        return TopicBuilder.name("AdEvaluationComplete").partitions(3).replicas(3).build();
    }

    // Claude 추가: /smp-msg 테스트 엔드포인트용 토픽
    @Bean
    public NewTopic simpleMessageTopic() {
        return TopicBuilder.name("simpleMessage").partitions(3).replicas(3).build();
    }

    // Claude 추가: Producer.pub()/pubString()이 쓰는 topicName("defaultTopic")도 기동 시 미리 생성
    @Bean
    public NewTopic defaultTopic() {
        return TopicBuilder.name("defaultTopic").partitions(3).replicas(3).build();
    }

    // Claude 추가: StreamService의 KStream-KStream join 데모가 구독하는 소스 토픽.
    // 이게 없으면 MissingSourceTopicException으로 앱 전체 Kafka Streams 클라이언트가 죽음.
    // 둘이 서로 join되는 관계라 co-partitioning을 위해 파티션 수를 동일하게 맞춤.
    @Bean
    public NewTopic leftTopic() {
        return TopicBuilder.name("leftTopic").partitions(3).replicas(3).build();
    }

    @Bean
    public NewTopic rightTopic() {
        return TopicBuilder.name("rightTopic").partitions(3).replicas(3).build();
    }

//
//    @Bean
//    public ConsumerFactory<String, Object> ConsumerFactory() {
//        Map<String, Object> myConfig = new HashMap<>();
//        myConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "13.125.129.151:9092, 3.39.236.110:9092, 13.125.110.158:9092");
//        myConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        myConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        return new DefaultKafkaConsumerFactory<>(myConfig);
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, Object> myfactory = new ConcurrentKafkaListenerContainerFactory<>();
//        myfactory.setConsumerFactory(ConsumerFactory());
//        return myfactory;
//    }

}
