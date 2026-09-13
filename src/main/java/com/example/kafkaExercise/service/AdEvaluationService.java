package com.example.kafkaExercise.service;

import com.example.kafkaExercise.vo.EffectOrNot;
import com.example.kafkaExercise.vo.PurchaseLog;
import com.example.kafkaExercise.vo.PurchaseLogOneProduct;
import com.example.kafkaExercise.vo.WatchingAdLog;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@RequiredArgsConstructor
public class AdEvaluationService {
    // 광고 데이터 중복 Join될 필요없다. --> Table
    // 광고 이력이 먼저 들어옵니다.
    // 구매 이력은 상품별로 들어오지 않습니다. (복수개의 상품 존재) --> contain
    // 광고에 머문시간이 10초 이상되어야만 join 대상
    // 특정가격이상의 상품은 join 대상에서 제외 (100만원)
    // 광고이력 : KTable(AdLog), 구매이력 : KTable(PurchaseLogOneProduct)
    // filtering, 형 변환,
    // EffectOrNot --> Json 형태로 Topic : AdEvaluationComplete
//    @Autowired
//    Producer myprdc;
    private final Producer myprdc;

    @Autowired
    public void buildPipeline(StreamsBuilder sb) {

        //object의 형태별로 Serializer, Deserializer 를 설정합니다.
        JsonSerializer<EffectOrNot> effectSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLog> purchaseLogSerializer = new JsonSerializer<>();
        JsonSerializer<WatchingAdLog> watchingAdLogSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLogOneProduct> purchaseLogOneProductSerializer = new JsonSerializer<>();

        JsonDeserializer<EffectOrNot> effectDeserializer = new JsonDeserializer<>(EffectOrNot.class);
        JsonDeserializer<PurchaseLog> purchaseLogDeserializer = new JsonDeserializer<>(PurchaseLog.class);
        JsonDeserializer<WatchingAdLog> watchingAdLogJsonDeserializer = new JsonDeserializer<>(WatchingAdLog.class);
        JsonDeserializer<PurchaseLogOneProduct> purchaseLogOneProductDeserializer = new JsonDeserializer<>(PurchaseLogOneProduct.class);

        Serde<EffectOrNot> effectOrNotSerde = Serdes.serdeFrom(effectSerializer, effectDeserializer);
        Serde<PurchaseLog> purchaseLogSerde = Serdes.serdeFrom(purchaseLogSerializer, purchaseLogDeserializer);
        Serde<WatchingAdLog> watchingAdLogSerde = Serdes.serdeFrom(watchingAdLogSerializer, watchingAdLogJsonDeserializer);
        Serde<PurchaseLogOneProduct> purchaseLogOneProductSerdeSerde = Serdes.serdeFrom(purchaseLogOneProductSerializer, purchaseLogOneProductDeserializer);

        // adLog topic 을 consuming 하여 KTable 로 받습니다.
        KTable<String, WatchingAdLog> adTable = sb.stream("adLog", Consumed.with(Serdes.String(), watchingAdLogSerde))
                .selectKey((k, v) -> v.getUserId() + "_" + v.getProductId()) // key를 userId+prodId로 생성해줍니다.
                .filter((k, v) -> Integer.parseInt(v.getWatchingTime()) > 10) // 광고 시청시간이 10초 이상인 데이터만 Table에 담습니다.
                .toTable(Materialized.<String, WatchingAdLog, KeyValueStore<Bytes, byte[]>>as("adStore") // Key-Value Store로 만들어줍니다.
                        .withKeySerde(Serdes.String())
                        .withValueSerde(watchingAdLogSerde)
                );

        // purchaseLog topic 을 consuming 하여 KStream 으로 받습니다.
        KStream<String, PurchaseLog> purchaseLogKStream = sb.stream("purchaseLog", Consumed.with(Serdes.String(), purchaseLogSerde));

        // 해당 KStream의 매 Row(Msg)마다 하기의 내용을 수행합니다.
        purchaseLogKStream.foreach((k, v) -> {

                    // value 의 product 개수만큼 반복하여 신규 VO에 값을 Binding 합니다.
                    for (Map<String, String> prodInfo : v.getProductInfo()) {
                        // price 100000 미만인 경우만 하기의 내용을 수행합니다. 위 purchaseLogKStream에서 filter조건을 주고 받아도 무관합니다.
                        if (Integer.parseInt(prodInfo.get("price")) < 1000000) {
                            PurchaseLogOneProduct tempVo = new PurchaseLogOneProduct();
                            tempVo.setUserId(v.getUserId());
                            tempVo.setProductId(prodInfo.get("productId"));
                            tempVo.setOrderId(v.getOrderId());
                            tempVo.setPrice(prodInfo.get("price"));
                            tempVo.setPurchasedDt(v.getPurchasedDt());

                            // 1개의 product로 나눈 데이터를 purchaseLogOneProduct Topic으로 produce 합니다.
                            myprdc.sendJoinedMsg("purchaseLogOneProduct", tempVo);

                            // 하기의 method는 samplie Data를 생산하여 Topic에 넣습니다. 1개 받으면 여러개를 생성하기 때문에 무한하게 생성됩니다. .
                            // sendNewMsg();
                        }
                    }
                }
        );

        // product이 1개씩 나누어 producing 된 topic을 읽어 KTable 로 받아옵니다.
        KTable<String, PurchaseLogOneProduct> purchaseLogOneProductKTable = sb.stream("purchaseLogOneProduct", Consumed.with(Serdes.String(), purchaseLogOneProductSerdeSerde))
                // Stream의 key 지정
                .selectKey((k, v) -> v.getUserId() + "_" + v.getProductId())
                // key-value Store로 이용이 가능하도록 생성
                .toTable(Materialized.<String, PurchaseLogOneProduct, KeyValueStore<Bytes, byte[]>>as("purchaseLogStore")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(purchaseLogOneProductSerdeSerde)
                );

        // value joiner 를 통해 Left, Right 값을 통한 Output 결과값을 bind 하거나 join 조건을 설정할 수 있습니다.
        ValueJoiner<WatchingAdLog, PurchaseLogOneProduct, EffectOrNot> tableStreamJoiner = (leftValue, rightValue) -> {
            EffectOrNot returnValue = new EffectOrNot();
            returnValue.setUserId(rightValue.getUserId());
            returnValue.setAdId(leftValue.getAdId());
            returnValue.setOrderId(rightValue.getOrderId());
            Map<String, String> tempProdInfo = new HashMap<>();
            tempProdInfo.put("productId", rightValue.getProductId());
            tempProdInfo.put("price", rightValue.getPrice());
            returnValue.setProductInfo(tempProdInfo);
            System.out.println("Joined!");
            return returnValue;
        };

        // table과 joiner를 입력해줍니다. stream join이 아니기에 window 설정은 불필요합니다.
        adTable.join(purchaseLogOneProductKTable, tableStreamJoiner)
                // join 이 완료된 데이터를 AdEvaluationComplete Topic으로 전달합니다.
                .toStream().to("AdEvaluationComplete", Produced.with(Serdes.String(), effectOrNotSerde));
    }

    // Claude 추가(상세 주석): adLog + purchaseLog 한 쌍을 랜덤 값으로 자동 생성해서 실제로 카프카에
    // produce하는 테스트용 헬퍼. AdEvaluationService 파이프라인을 손으로 JSON 안 만들고 바로 테스트하려고
    // 만든 것으로 보이나, 원래 이 클래스/다른 코드 어디에서도 호출되지 않던 "고아 메서드"였음.
    // 이번에 ProducerController의 POST /gen-test-data 에 연결해서 실제로 호출 가능하게 만듦.
    public void sendNewMsg() {
        PurchaseLog tempPurchaseLog = new PurchaseLog();
        WatchingAdLog tempWatchingAdLog = new WatchingAdLog();

        //랜덤한 ID를 생성하기 위해 아래의 함수를 사용합니다.
        // random Numbers for concatenation with attrs
        Random rd = new Random();
        int rdUidNumber = rd.nextInt(9999);       // 유저 번호 (adLog/purchaseLog 둘 다 이 번호로 userId를 맞춤 -> join key 일치 보장)
        int rdOrderNumber = rd.nextInt(9999);      // 주문 번호 (purchaseLog의 orderId에만 사용)
        int rdProdIdNumber = rd.nextInt(9999);     // 상품 번호 (adLog의 productId & purchaseLog 상품 중 1개를 이 번호로 맞춤 -> join key 일치 보장)
        int rdPriceIdNumber = rd.nextInt(90000) + 10000; // 참고: 지금은 안 쓰임(가격은 아래 반복문 안에서 매번 새로 뽑음)
        int prodCnt = rd.nextInt(9) + 1;           // 이번 주문에 담을 상품 개수 (1~9개)
        int watchingTime = rd.nextInt(55) + 5;     // 광고 시청 시간 5~59초 (10초 이하로 나오면 AdEvaluationService의
                                                    // filter(watchingTime > 10) 조건에서 걸러져서 조인이 안 될 수 있음)

        // bind value for purchaseLog
        tempPurchaseLog.setUserId("uid-" + String.format("%05d", rdUidNumber));
        tempPurchaseLog.setPurchasedDt("20230101070000"); // 날짜는 고정값(랜덤 아님, 테스트용이라 의미 없어서 안 바꾼 듯)
        tempPurchaseLog.setOrderId("od-" + String.format("%05d", rdOrderNumber));

        // Claude 수정: 원래 코드는 tempProd(Map)를 반복문 밖에서 한 번만 만들고 매번 그 안의 값만
        // 덮어써서 리스트에 넣었기 때문에, 실제로는 "같은 상품이 prodCnt번 중복"되어 들어갔음
        // (Map은 참조 타입이라 나중에 값을 바꿔도 리스트 안 다른 원소들까지 같이 바뀌는 거나 마찬가지).
        // -> 매 반복마다 새 Map을 만들도록 수정. 단, adLog와 매칭을 보장하기 위해 첫 번째 상품(i=0)만은
        // adLog의 productId(rdProdIdNumber)와 반드시 같게 유지하고, 나머지는 서로 다른 랜덤 상품으로 생성.
        ArrayList<Map<String, String>> tempProdInfo = new ArrayList<>();

        for (int i = 0; i < prodCnt; i++) {
            Map<String, String> tempProd = new HashMap<>();
            String productId = (i == 0)
                    ? "pg-" + String.format("%05d", rdProdIdNumber) // adLog와 매칭되는 상품
                    : "pg-" + String.format("%05d", rd.nextInt(9999)); // 매칭 안 되는 나머지 상품들
            tempProd.put("productId", productId);
            tempProd.put("price", String.format("%05d", rd.nextInt(90000) + 10000));
            tempProdInfo.add(tempProd);
        }
        tempPurchaseLog.setProductInfo(tempProdInfo);

        // bind value for watchingAdLog
        // userId, productId를 위 purchaseLog와 똑같은 랜덤 번호(rdUidNumber, rdProdIdNumber)로 맞춰서
        // AdEvaluationService의 join key(userId_productId)가 반드시 일치하도록 의도적으로 구성함.
        tempWatchingAdLog.setUserId("uid-" + String.format("%05d", rdUidNumber));
        tempWatchingAdLog.setProductId("pg-" + String.format("%05d", rdProdIdNumber));
        tempWatchingAdLog.setAdId("ad-" + String.format("%05d", rdUidNumber)); // adId는 별도 규칙 없이 그냥 uid 번호 재사용
        tempWatchingAdLog.setAdType("banner");
        tempWatchingAdLog.setWatchingTime(String.valueOf(watchingTime));
        tempWatchingAdLog.setWatchingDt("20230201070000"); // 날짜도 고정값(랜덤 아님)

        // produce msg - 순서상 purchaseLog를 먼저 보내지만, 어차피 두 토픽 다 KTable로 받고 시간창 없는
        // KTable-KTable join이라 어느 쪽이 먼저/나중에 와도 상관없이 나중에 join된다(StreamService의
        // KStream-KStream join처럼 "10초 이내에 순서 상관없이 와야 하는" 제약이 없음).
        myprdc.sendMsgForPurchaseLog("purchaseLog", tempPurchaseLog);
        myprdc.sendMsgForWatchingAdLog("adLog", tempWatchingAdLog);
    }
}
