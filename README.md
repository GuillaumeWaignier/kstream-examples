# kstream-examples

[![Build status](https://travis-ci.org/GuillaumeWaignier/kstream-examples.svg?branch=master)](https://travis-ci.org/GuillaumeWaignier/kstream-examples) [![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=org.ianitrix.kstream.example%3Akstream-examples&metric=alert_status)](https://sonarcloud.io/dashboard/index/org.ianitrix.kstream:kstream-examples)


topic price:

 

kafka-console-producer  --broker-list localhost:9092 --topic prices --property "parse.key=true"  --property "key.separator=;"
{"productId":{"productId":"tv"},"storeId":{"storeId":"Lille"}};{"priceHT":10}
{"productId":{"productId":"tv"},"storeId":{"storeId":"Paris"}};{"priceHT":1000}
{"productId":{"productId":"tv"},"storeId":{"storeId":"Nice"}};{"priceHT":100}
{"productId":{"productId":"phone"},"storeId":{"storeId":"Lille"}};{"priceHT":3}
{"productId":{"productId":"phone"},"storeId":{"storeId":"Paris"}};{"priceHT":300}
{"productId":{"productId":"phone"},"storeId":{"storeId":"Nice"}};{"priceHT":50}


kafka-console-producer  --broker-list localhost:9092 --topic sales --property "parse.key=true"  --property "key.separator=;"
{"saleID":"1"};{"storeId":{"storeId":"Lille"},"customerId":"Paul","totalPrice":10.21,"saleLines":[{"productId":{"productId":"tv"},"quantity":2},{"productId":{"productId":"phone"},"quantity":1}]}
{"saleID":"2"};{"storeId":{"storeId":"Paris"},"customerId":"Marc","totalPrice":10.21,"saleLines":[{"productId":{"productId":"tv"},"quantity":1}]}
{"saleID":"1"};{"storeId":{"storeId":"Lille"},"customerId":"Paul","totalPrice":10.21,"saleLines":[{"productId":{"productId":"tv"},"quantity":1},{"productId":{"productId":"phone"},"quantity":2}]}

