package org.ianitrix.kstream.examples.pojo.json;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PricedProductLineKey {

    private ProductKey productId;
    private StoreKey storeId;
    private SaleKey saleId;
}
