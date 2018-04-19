package com.skynyrd.kafka.transform;

import com.skynyrd.kafka.transform.impl.ProdAttrsRecordTransformer;
import com.skynyrd.kafka.transform.impl.BaseProductsRecordTransformer;
import com.skynyrd.kafka.transform.impl.ProductsRecordTransformer;
import com.skynyrd.kafka.transform.impl.StoresRecordTransformer;

import java.util.Optional;

public class RecordTransformerFactory {
    private static StoresRecordTransformer storesRecordTransformer = new StoresRecordTransformer();
    private static BaseProductsRecordTransformer baseProductsRecordTransformer = new BaseProductsRecordTransformer();
    private static ProductsRecordTransformer productsRecordTransformer = new ProductsRecordTransformer();
    private static ProdAttrsRecordTransformer prodAttrsRecordTransformer = new ProdAttrsRecordTransformer();

    public static Optional<AbstractRecordTransformer> getTransformer(String table) throws IllegalArgumentException {
        switch (table) {
            case "stores":
                return Optional.of(storesRecordTransformer);
            case "base_products":
                return Optional.of(baseProductsRecordTransformer);
            case "products":
                return Optional.of(productsRecordTransformer);
            case "prod_attr_values":
                return Optional.of(prodAttrsRecordTransformer);
            default:
                return Optional.empty();
        }
    }
}
