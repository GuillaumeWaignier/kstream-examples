package org.ianitrix.kstream.examples.pojo.json;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Unique Key for a sale.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SaleKey {

    private String saleID;
}
