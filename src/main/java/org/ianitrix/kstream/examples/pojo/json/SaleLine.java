package org.ianitrix.kstream.examples.pojo.json;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SaleLine {
    private ProductKey productId;
    private int quantity;
}
