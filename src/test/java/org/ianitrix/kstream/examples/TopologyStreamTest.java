package org.ianitrix.kstream.examples;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.ianitrix.kstream.examples.pojo.json.*;
import org.junit.Assert;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Properties;

public class TopologyStreamTest {

    private TopologyTestDriver testDriver;

    //serdes
    private final Serde<PriceKey> priceKeySerde = SerdesUtils.createJsonSerdes(PriceKey.class);
    private final Serde<Price> priceSerde = SerdesUtils.createJsonSerdes(Price.class);
    private final Serde<SaleKey> saleKeySerde = SerdesUtils.createJsonSerdes(SaleKey.class);
    private final Serde<Sale> saleSerde = SerdesUtils.createJsonSerdes(Sale.class);

    // consumer used to mock the input stream
    private ConsumerRecordFactory<PriceKey, Price> priceRecordFactory = new ConsumerRecordFactory<>(priceKeySerde.serializer(), priceSerde.serializer());
    private ConsumerRecordFactory<SaleKey, Sale> saleRecordFactory = new ConsumerRecordFactory<>(saleKeySerde.serializer(), saleSerde.serializer());

    // Mock Data
    final StoreKey storeKeyLille = new StoreKey("Lille");
    final StoreKey storeKeyParis = new StoreKey("Paris");
    final StoreKey storeKeyNice = new StoreKey("Nice");
    final ProductKey productKeyTv = new ProductKey("tv");
    final ProductKey productKeyPhone = new ProductKey("phone");

    @BeforeEach
    public void setup() {
        final TopologyStreamBuilder builder = new TopologyStreamBuilder();
        final Topology topology = builder.buildStream();

        final Properties config = Main.getStreamConfig();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

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

        ProducerRecord<String, String> outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "20.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "3.0");

        //Update total accordingly to the second sale
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "1020.0");

        //Update total because now there is only one TV in sale '1' instead of two
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "1000.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "1000.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "1000.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "1010.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "0.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "0.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "0.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "6.0");

        Assert.assertNull(testDriver.readOutput(
                TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer()));
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
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyTvParis, new Price(10)));


        ProducerRecord<String, String> outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "20.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=phone)", "3.0");

        //Update total accordingly to the second sale
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "1020.0");

        //Update total because now the TV in Paris is only 10 instead of 1000
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "20.0");
        outputRecord = testDriver.readOutput(TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer());
        OutputVerifier.compareKeyValue(outputRecord, "ProductKey(productId=tv)", "30.0");


        Assert.assertNull(testDriver.readOutput(
                TopologyStreamBuilder.TOPIC_TOTAL_PRICE, new StringDeserializer(), new StringDeserializer()));
    }

    private void sendPriceDataIntoTopic() {
        // Mock data
        final PriceKey priceKeyTvLille = new PriceKey(productKeyTv, storeKeyLille);
        final PriceKey priceKeyTvParis = new PriceKey(productKeyTv, storeKeyParis);
        final PriceKey priceKeyTvNice = new PriceKey(productKeyTv, storeKeyNice);
        final PriceKey priceKeyPhoneLille = new PriceKey(productKeyPhone, storeKeyLille);
        final PriceKey priceKeyPhoneParis = new PriceKey(productKeyPhone, storeKeyParis);

        // simulate price data in input topic
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyTvLille, new Price(10)));
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyTvNice, new Price(100)));
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyTvParis, new Price(1000)));
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyPhoneLille, new Price(3)));
        testDriver.pipeInput(priceRecordFactory.create(TopologyStreamBuilder.TOPIC_PRICE, priceKeyPhoneParis, new Price(300)));
    }
}