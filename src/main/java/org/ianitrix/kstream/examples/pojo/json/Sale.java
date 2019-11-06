package org.ianitrix.kstream.examples.pojo.json;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;

/**
 * A Sale in a given store.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Sale {

    private StoreKey storeId;
    private String customerId;
    private List<SaleLine> saleLines = new LinkedList<>();
    private double totalPrice;
}
