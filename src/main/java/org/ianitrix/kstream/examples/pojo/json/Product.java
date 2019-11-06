package org.ianitrix.kstream.examples.pojo.json;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Product {

    private String productId;
    private String brand;
    private String name;
    private Double vat;
}
