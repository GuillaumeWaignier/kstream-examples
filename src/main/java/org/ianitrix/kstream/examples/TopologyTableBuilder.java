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

public class TopologyTableBuilder {


    /**
     * Same as the stream version.
     * @return
     */
    public Topology buildTable() {
        StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();

        final KTable<PriceKey, Price> prices = builder.table("prices", Consumed.with(SerdesUtils.createJsonSerdes(PriceKey.class), SerdesUtils.createJsonSerdes(Price.class)));

        final KStream<SaleKey, Sale> sales = builder.stream("sales", Consumed.with(SerdesUtils.createJsonSerdes(SaleKey.class), SerdesUtils.createJsonSerdes(Sale.class)));


        // total quantity of products sales by store
        final KStream<PricedProductLineKey, SaleLine> productSaleLines = sales.flatMap((saleKey, sale) -> this.createPricedSaleLines(saleKey, sale));
        final KGroupedStream<PricedProductLineKey, SaleLine> productKeySaleLineKGroupedStream = productSaleLines.groupByKey(Grouped.with(SerdesUtils.createJsonSerdes(PricedProductLineKey.class), SerdesUtils.createJsonSerdes(SaleLine.class)));
        final KTable<PricedProductLineKey, SaleLine> reduce = productKeySaleLineKGroupedStream.reduce((saleLine, v1) -> v1);
        final KGroupedTable<PriceKey, SaleLine> priceKeySaleLineKGroupedTable = reduce.groupBy((pricedProductLineKey, saleLine) -> KeyValue.pair(new PriceKey(pricedProductLineKey.getProductId(), pricedProductLineKey.getStoreId()), saleLine),
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


    private List<KeyValue<PriceKey,SaleLine>> createSaleLines(final SaleKey saleKey, final Sale sale) {
        final List<KeyValue<PriceKey,SaleLine>> saleLines = new LinkedList<>();

        sale.getSaleLines().forEach(saleLine -> saleLines.add(KeyValue.pair(new PriceKey(saleLine.getProductId(), sale.getStoreId()), saleLine)));

        return saleLines;
    }

    private List<KeyValue<PricedProductLineKey,SaleLine>> createPricedSaleLines(final SaleKey saleKey, final Sale sale) {
        final List<KeyValue<PricedProductLineKey,SaleLine>> saleLines = new LinkedList<>();

        sale.getSaleLines().forEach(saleLine -> saleLines.add(KeyValue.pair(new PricedProductLineKey(saleLine.getProductId(), sale.getStoreId(), saleKey), saleLine)));

        return saleLines;
    }
}
