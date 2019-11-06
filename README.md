# kstream-examples

topic price:



{"productId":{"productId":"tv"},"storeId":{"storeId":"Lille"}};{"priceHT":100}
{"productId":{"productId":"tv"},"storeId":{"storeId":"Paris"}};{"priceHT":1000}
{"productId":{"productId":"tv"},"storeId":{"storeId":"Nice"}};{"priceHT":500}
{"productId":{"productId":"frigo"},"storeId":{"storeId":"Lille"}};{"priceHT":10}
{"productId":{"productId":"frigo"},"storeId":{"storeId":"Paris"}};{"priceHT":100}
{"productId":{"productId":"frigo"},"storeId":{"storeId":"Nice"}};{"priceHT":50}


kafka-console-producer  --broker-list localhost:9092 --topic sales --property "parse.key=true"  --property "key.separator=;"
{"saleID":"1"};{"storeId":{"storeId":"Lille"},"customerId":"marc","totalPrice":10.21,"saleLines":[{"productId":{"productId":"tv"},"quantity":2},{"productId":{"productId":"frigo"},"quantity":1}]}
{"saleID":"2"};{"storeId":{"storeId":"Paris"},"customerId":"marc","totalPrice":10.21,"saleLines":[{"productId":{"productId":"tv"},"quantity":1}]}
{"saleID":"1"};{"storeId":{"storeId":"Lille"},"customerId":"marc","totalPrice":10.21,"saleLines":[{"productId":{"productId":"tv"},"quantity":1},{"productId":{"productId":"frigo"},"quantity":2}]}
