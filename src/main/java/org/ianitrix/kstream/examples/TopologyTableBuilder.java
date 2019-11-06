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

import java.util.LinkedList;
import java.util.List;

/**
 * Same as the stream version, but now the sale or not immutable.
 * When we update a sale with a existing saleID, the total price by product is update:
 * Indeed, thanks to the KTable and the aggregation, the impact of the previous version of the sale is remove
 * and then the new version of the sale is added.
 */
public class TopologyTableBuilder {

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
    public Topology buildTable() {
        StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();

        // a ktable that contains all prices for each tuple{productId,storeId}
        // same as TopologyStreamBuilder
        final KTable<PriceKey, Price> prices = builder.table(TOPIC_PRICE, Consumed.with(SerdesUtils.createJsonSerdes(PriceKey.class), SerdesUtils.createJsonSerdes(Price.class)));

        // a kstream that contains mutable sales
        // Now, we can update a sale by sending a sale with the same saleId.
        // However, we do not create a ktable for a sale, because do not need to keep the previous complete sale (only each line of sale is needed)
        final KStream<SaleKey, Sale> sales = builder.stream(TOPIC_SALE, Consumed.with(SerdesUtils.createJsonSerdes(SaleKey.class), SerdesUtils.createJsonSerdes(Sale.class)));


        // 1. input sale -> output list of products (i.e., corresponding to lines of sale)
        // since a sale contains several products, we split the sale
        // since a line of sale does not contains any information about the sale, we also change the key in order to add storeId
        // Moreover, in addition we also add the saleId in the key in order to be able to update a sale line of a given sale
        final KStream<PricedProductLineKey, SaleLine> productSaleLines = sales.flatMap((saleKey, sale) -> this.createPricedSaleLines(saleKey, sale));

        // 2. we group the line of sale by the price of key
        // Now, for each tuple{productId,storeId, saleId}, we have the corresponding line of sales
        final KGroupedStream<PricedProductLineKey, SaleLine> groupedProductSaleLines = productSaleLines.groupByKey(Grouped.with(SerdesUtils.createJsonSerdes(PricedProductLineKey.class), SerdesUtils.createJsonSerdes(SaleLine.class)));

        //3. we create a ktable in order to keep, for each line of sale, the last version.
        final KTable<PricedProductLineKey, SaleLine> tableProductSaleLines = groupedProductSaleLines.reduce((saleLine, v1) -> v1);


        final KGroupedTable<PriceKey, SaleLine> priceKeySaleLineKGroupedTable = tableProductSaleLines.groupBy((pricedProductLineKey, saleLine) -> KeyValue.pair(new PriceKey(pricedProductLineKey.getProductId(), pricedProductLineKey.getStoreId()), saleLine),
                Grouped.with(SerdesUtils.createJsonSerdes(PriceKey.class), SerdesUtils.createJsonSerdes(SaleLine.class)));

        final KTable<PriceKey, Double> saleNumber = priceKeySaleLineKGroupedTable.aggregate(() -> 0.0,
                (aggKey, newValue, aggValue) -> aggValue + newValue.getQuantity(), /* adder */
                (aggKey, oldValue, aggValue) -> aggValue - oldValue.getQuantity(), /* subtractor */
                Materialized.<PriceKey, Double, KeyValueStore<Bytes, byte[]>>as("tablePriceByStore" /* state store name */)
                        .withKeySerde(SerdesUtils.createJsonSerdes(PriceKey.class)) /* key serde */
                        .withValueSerde(Serdes.Double()));/* serde for aggregate value */


        // Total prices by store
        final KTable<PriceKey, Double> totalPriceByStore = saleNumber.join(prices, (quantity, price) -> quantity * price.getPriceHT());
        totalPriceByStore.toStream().map((priceKey, price) -> KeyValue.pair(priceKey.toString(), String.valueOf(price)))
                .to("tableTotalPriceByStore", Produced.with(stringSerde, stringSerde));

        final KGroupedTable<ProductKey, Double> productKeyDoubleKGroupedStream = totalPriceByStore.groupBy((priceKey, totalPrice) -> KeyValue.pair(priceKey.getProductId(), totalPrice), Grouped.with(SerdesUtils.createJsonSerdes(ProductKey.class), Serdes.Double()));
        final KTable<ProductKey, Double> totalSale = productKeyDoubleKGroupedStream.aggregate(() -> 0.0,
                (productKey, totalPrice, newTotalPrice) -> newTotalPrice + totalPrice,
                (productKey, oldPrice, newTotalPrice) -> newTotalPrice - oldPrice,
                Materialized.<ProductKey, Double, KeyValueStore<Bytes, byte[]>>as("tableTotalSale" /* state store name */)
                        .withKeySerde(SerdesUtils.createJsonSerdes(ProductKey.class)) /* key serde */
                        .withValueSerde(Serdes.Double()));
        totalSale.toStream().map((productKey, price) -> KeyValue.pair(productKey.toString(), String.valueOf(price)))
                .to("tableTotalPrice", Produced.with(stringSerde, stringSerde));


        return builder.build();
    }

    private List<KeyValue<PricedProductLineKey,SaleLine>> createPricedSaleLines(final SaleKey saleKey, final Sale sale) {
        final List<KeyValue<PricedProductLineKey,SaleLine>> saleLines = new LinkedList<>();

        sale.getSaleLines().forEach(saleLine -> saleLines.add(KeyValue.pair(new PricedProductLineKey(saleLine.getProductId(), sale.getStoreId(), saleKey), saleLine)));

        return saleLines;
    }
}
