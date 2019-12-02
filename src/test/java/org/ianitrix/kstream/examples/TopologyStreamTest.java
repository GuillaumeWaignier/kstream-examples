package org.ianitrix.kstream.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.ianitrix.kstream.examples.pojo.json.*;
import org.junit.Assert;
import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class TopologyStreamTest {

    private TopologyTestDriver testDriver;

    //serdes
    private final Serde<PriceKey> priceKeySerde = SerdesUtils.createJsonSerdes(PriceKey.class);
    private final Serde<Price> priceSerde = SerdesUtils.createJsonSerdes(Price.class);
    private final Serde<SaleKey> saleKeySerde = SerdesUtils.createJsonSerdes(SaleKey.class);
    private final Serde<Sale> saleSerde = SerdesUtils.createJsonSerdes(Sale.class);

    // consumer used to mock the input stream
    private final ConsumerRecordFactory<PriceKey, Price> priceRecordFactory = new ConsumerRecordFactory<>(priceKeySerde.serializer(), priceSerde.serializer());
    private final ConsumerRecordFactory<SaleKey, Sale> saleRecordFactory = new ConsumerRecordFactory<>(saleKeySerde.serializer(), saleSerde.serializer());

    // Mock Data
    private final StoreKey storeKeyLille = new StoreKey("Lille");
    private final StoreKey storeKeyParis = new StoreKey("Paris");
    private final StoreKey storeKeyNice = new StoreKey("Nice");
    private final ProductKey productKeyTv = new ProductKey("tv");
    private final ProductKey productKeyPhone = new ProductKey("phone");
    private final PriceKey priceKeyTvLille = new PriceKey(productKeyTv, storeKeyLille);
    private final PriceKey priceKeyTvParis = new PriceKey(productKeyTv, storeKeyParis);
    private final PriceKey priceKeyTvNice = new PriceKey(productKeyTv, storeKeyNice);
    private final PriceKey priceKeyPhoneLille = new PriceKey(productKeyPhone, storeKeyLille);
    private final PriceKey priceKeyPhoneParis = new PriceKey(productKeyPhone, storeKeyParis);

    @BeforeEach
    public void setup() {
        final TopologyStreamBuilder builder = new TopologyStreamBuilder();
        final Topology topology = builder.buildStream();
        log.info(topology.describe().toString());

        final Properties config = Main.getStreamConfig();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 111110);

        testDriver = new TopologyTestDriver(topology, config);

        this.sendPriceDataIntoTopic();
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void testOneSaleAtLille() {

        // simulate sale
        final Sale sale1 = new Sale(storeKeyLille, "Paul", List.of(new SaleLine(productKeyTv, 2), new SaleLine(productKeyPhone, 1)), 12);

        // simulate send data in input topics
        testDriver.pipeInput(saleRecordFactory.create(TopologyStreamBuilder.TOPIC_SALE, new SaleKey("1"), sale1));

        ProducerRecord<String, String> outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "20.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "3.0");

        Assert.assertNull(testDriver.readOutput(
                TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer()));

    }

    @Test
    public void testOneSaleAtLilleAndOneAtParis() {

        // simulate 2 sales
        final Sale sale1 = new Sale(storeKeyLille, "Paul", List.of(new SaleLine(productKeyTv, 2), new SaleLine(productKeyPhone, 1)), 12);
        final Sale sale2 = new Sale(storeKeyParis, "Marc", List.of(new SaleLine(productKeyTv, 1)), 12);

        // simulate send data in input topics
        testDriver.pipeInput(saleRecordFactory.create(TopologyStreamBuilder.TOPIC_SALE, new SaleKey("1"), sale1));
        testDriver.pipeInput(saleRecordFactory.create(TopologyStreamBuilder.TOPIC_SALE, new SaleKey("2"), sale2));

        ProducerRecord<String, String> outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "20.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "3.0");

        //Update total accordingly to the second sale
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "1020.0");

        Assert.assertNull(testDriver.readOutput(
                TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer()));
    }

    @Test
    public void testTwoSalesAndThenUpdateTheFirst() {

        // simulate 2 sales
        final Sale sale1 = new Sale(storeKeyLille, "Paul", List.of(new SaleLine(productKeyTv, 2), new SaleLine(productKeyPhone, 1)), 12);
        final Sale sale2 = new Sale(storeKeyParis, "Marc", List.of(new SaleLine(productKeyTv, 1)), 12);
        // update sale1 to fix a error in the sale
        final Sale sale1Version2 = new Sale(storeKeyLille, "Paul", List.of(new SaleLine(productKeyTv, 1), new SaleLine(productKeyPhone, 2)), 12);

        // simulate send data in input topics
        testDriver.pipeInput(saleRecordFactory.create(TopologyStreamBuilder.TOPIC_SALE, new SaleKey("1"), sale1));
        testDriver.pipeInput(saleRecordFactory.create(TopologyStreamBuilder.TOPIC_SALE, new SaleKey("2"), sale2));
        testDriver.pipeInput(saleRecordFactory.create(TopologyStreamBuilder.TOPIC_SALE, new SaleKey("1"), sale1Version2));


        // Check the number of product sale by store
        final KeyValueStore<PriceKey, Long> quantitySaleProduct = testDriver.getKeyValueStore("quantitySaleProduct");
        Assertions.assertEquals(3, quantitySaleProduct.get(this.priceKeyTvLille));
        Assertions.assertEquals(1, quantitySaleProduct.get(this.priceKeyTvParis));
        Assertions.assertEquals(3, quantitySaleProduct.get(this.priceKeyPhoneLille));

        final KeyValueStore<PriceKey, Double> priceSaleProduct = testDriver.getKeyValueStore("priceSaleProduct");
        Assertions.assertEquals(30.0, priceSaleProduct.get(this.priceKeyTvLille));
        Assertions.assertEquals(1000.0, priceSaleProduct.get(this.priceKeyTvParis));
        Assertions.assertEquals(9.0, priceSaleProduct.get(this.priceKeyPhoneLille));

        final KeyValueStore<ProductKey, Double> totalSale = testDriver.getKeyValueStore("totalSale");
        Assertions.assertEquals(1030.0, totalSale.get(this.productKeyTv));
        Assertions.assertEquals(9.0, totalSale.get(this.productKeyPhone));

        // Check the output of kafka
        ProducerRecord<String, String> outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "20.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "3.0");

        //Update total accordingly to the second sale
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "1020.0");

        //sale are immutable. The second version of sale1 is added to the previous total
        // We need to test all intermediate states
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "1000.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "1030.0");

        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "0.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "9.0");

        Assert.assertNull(testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer()));
    }

    @Test
    public void testTwoSalesAndThenUpdatePrice() {

        // simulate 2 sales
        final Sale sale1 = new Sale(storeKeyLille, "Paul", List.of(new SaleLine(productKeyTv, 2), new SaleLine(productKeyPhone, 1)), 12);
        final Sale sale2 = new Sale(storeKeyParis, "Marc", List.of(new SaleLine(productKeyTv, 1)), 12);

        // simulate send data in input topics
        testDriver.pipeInput(saleRecordFactory.create(TopologyStreamBuilder.TOPIC_SALE, new SaleKey("1"), sale1));
        testDriver.pipeInput(saleRecordFactory.create(TopologyStreamBuilder.TOPIC_SALE, new SaleKey("2"), sale2));

        // update price of tv for Paris
        final PriceKey priceKeyTvParis = new PriceKey(productKeyTv, storeKeyParis);
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyTvParis, new Price(33)));

        ProducerRecord<String, String> outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "20.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "3.0");

        //Update total accordingly to the second sale
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "1020.0");

        //Update total because now the TV in Paris is only 33 instead of 1000
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "20.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "53.0");


        Assert.assertNull(testDriver.readOutput(
                TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer()));
    }

    private void sendPriceDataIntoTopic() {

        // simulate price data in input topic
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyTvLille, new Price(10)));
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyTvNice, new Price(100)));
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyTvParis, new Price(1000)));
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyPhoneLille, new Price(3)));
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyPhoneParis, new Price(300)));
    }

    /**
     * If we read all topics content in one time, we get only the first message
     * @param topicName
     * @return
     */
    private Map<String, ProducerRecord<String, String>> readAllRecords(final String topicName) {
        final Map<String, ProducerRecord<String, String>> records = new HashMap<>();
        ProducerRecord<String, String> record = testDriver.readOutput(topicName, new StringDeserializer(), new StringDeserializer());

        while (record != null) {
            records.put(record.key(), record);
            record = testDriver.readOutput(TopologyTableBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        }
        return records;
    }
}