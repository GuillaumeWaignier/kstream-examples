package org.ianitrix.kstream.examples;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.ianitrix.kstream.examples.pojo.json.*;

import java.util.*;

/**
 * Build a stream that compute the total price for each kind of sale items.
 */
public class TopologyStreamBuilder {

    /**
     * Topic name that contains prices
     */
    public static final String TOPIC_PRICE = "prices";

    /**
     * Topic name that contains sales
     */
    public static final String TOPIC_SALE = "sales";


    /**
     * Build the topology
     * @return topology
     */
    public Topology buildStream() {
        StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();

        // a ktable that contains all prices for each tuple{productId,storeId}
        final KTable<PriceKey, Price> prices = builder.table(TOPIC_PRICE, Consumed.with(SerdesUtils.createJsonSerdes(PriceKey.class), SerdesUtils.createJsonSerdes(Price.class)));

        // a kstream that contains immutable sales
        final KStream<SaleKey, Sale> sales = builder.stream(TOPIC_SALE, Consumed.with(SerdesUtils.createJsonSerdes(SaleKey.class), SerdesUtils.createJsonSerdes(Sale.class)));


        // 1. input sale -> output list of products (i.e., corresponding to lines of sale)
        // since a sale contains several products, we split the sale
        // since a line of sale does not contains any information about the sale, we also map the SaleKey into a PriceKey
        final KStream<PriceKey, SaleLine> productSaleLines = sales.flatMap((saleKey, sale) -> this.createSaleLines(saleKey, sale));

        // 2. we group the line of sale by the price of key
        // Now, for each tuple{productId,storeId}, we have the corresponding line of sales
        final KGroupedStream<PriceKey, SaleLine> groupedProductSaleLines = productSaleLines.groupByKey(Grouped.with(SerdesUtils.createJsonSerdes(PriceKey.class), SerdesUtils.createJsonSerdes(SaleLine.class)));

        // 3. input lines of sale for each tuple{productId,storeId} -> output quantity of sale product
        // We make a rolling aggregation by adding the quantity of sale product to the previously computed quantity for this product
        final KTable<PriceKey, Long> quantitySaleProduct = groupedProductSaleLines.aggregate(()->0L,
                (priceKey, saleLine, quantity) -> quantity + saleLine.getQuantity(),
                Materialized.<PriceKey, Long, KeyValueStore<Bytes, byte[]>>as("quantitySaleProduct" /* state store name */)
                        .withKeySerde(SerdesUtils.createJsonSerdes(PriceKey.class)) /* key serde */
                        .withValueSerde(Serdes.Long()));


        //4. we merge the quantity of sale product with the price
        // We obtain the total price for each tuple{productId,storeId}
        final KTable<PriceKey, Double> totalPriceByStore = quantitySaleProduct.join(prices, (quantity, price) -> quantity * price.getPriceHT());

        //Optional: For testing table content is streamed into a topic.
        //We convert the json into string
        totalPriceByStore.toStream().map((priceKey, price) -> KeyValue.pair(priceKey.toString(), String.valueOf(price)))
                .to("totalPriceByStore", Produced.with(stringSerde, stringSerde));


        //5. We want the total price by product (independently in which store the product is sale)
        final KGroupedStream<ProductKey, Double> productKeyDoubleKGroupedStream = totalPriceByStore.toStream().groupBy((priceKey, totalPrice) -> priceKey.getProductId(), Grouped.with(SerdesUtils.createJsonSerdes(ProductKey.class), Serdes.Double()));
        final KTable<ProductKey, Double> totalSale = productKeyDoubleKGroupedStream.aggregate(() -> 0.0,
                (productKey, totalPrice, newTotalPrice) -> newTotalPrice + totalPrice,
        Materialized.<ProductKey, Double, KeyValueStore<Bytes, byte[]>>as("totalSale" /* state store name */)
                .withKeySerde(SerdesUtils.createJsonSerdes(ProductKey.class)) /* key serde */
                .withValueSerde(Serdes.Double()));
        totalSale.toStream().map((productKey, price) -> KeyValue.pair(productKey.toString(), String.valueOf(price)))
                .to("totalPrice", Produced.with(stringSerde, stringSerde));


        return builder.build();
    }


    private List<KeyValue<PriceKey,SaleLine>> createSaleLines(final SaleKey saleKey, final Sale sale) {
        final List<KeyValue<PriceKey,SaleLine>> saleLines = new LinkedList<>();

        sale.getSaleLines().forEach(saleLine -> saleLines.add(KeyValue.pair(new PriceKey(saleLine.getProductId(), sale.getStoreId()), saleLine)));

        return saleLines;
    }
}
