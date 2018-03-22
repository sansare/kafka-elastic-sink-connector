package com.skynyrd.kafka.transform;

import com.skynyrd.kafka.transform.impl.ProdAttrsRecordTransformer;
import com.skynyrd.kafka.transform.impl.BaseProductsRecordTransformer;
import com.skynyrd.kafka.transform.impl.ProductsRecordTransformer;
import com.skynyrd.kafka.transform.impl.StoresRecordTransformer;

public class RecordTransformerFactory {
    private static StoresRecordTransformer storesRecordTransformer = new StoresRecordTransformer();
    private static BaseProductsRecordTransformer baseProductsRecordTransformer = new BaseProductsRecordTransformer();
    private static ProductsRecordTransformer productsRecordTransformer = new ProductsRecordTransformer();
    private static ProdAttrsRecordTransformer prodAttrsRecordTransformer = new ProdAttrsRecordTransformer();

    public static AbstractRecordTransformer getTransformer(String topic) throws IllegalArgumentException {
        switch (topic) {
            case "stores-pg.public.stores":
                return storesRecordTransformer;
            case "stores-pg.public.base_products":
                return baseProductsRecordTransformer;
            case "stores-pg.public.products":
                return productsRecordTransformer;
            case "stores-pg.public.prod_attr_values":
                return prodAttrsRecordTransformer;
            default:
                throw new IllegalArgumentException("Unknown topic: [" + topic + "]");
        }
    }
}
