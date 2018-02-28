package com.skynyrd.kafka.transform;

import com.skynyrd.kafka.transform.impl.ProductAttrsRecordTransformer;
import com.skynyrd.kafka.transform.impl.ProductsRecordTransformer;
import com.skynyrd.kafka.transform.impl.StoresRecordTransformer;

public class RecordTransformerFactory {
    private static StoresRecordTransformer storesRecordTransformer = new StoresRecordTransformer();
    private static ProductsRecordTransformer productsRecordTransformer = new ProductsRecordTransformer();
    private static ProductAttrsRecordTransformer productAttrsRecordTransformer = new ProductAttrsRecordTransformer();

    public static AbstractRecordTransformer getTransformer(String topic) throws IllegalArgumentException {
        switch (topic) {
            case "stores":
                return storesRecordTransformer;
            case "products":
                return productsRecordTransformer;
            case "product":
                return productAttrsRecordTransformer;
            default:
                throw new IllegalArgumentException("Unknown topic: [" + topic + "]");
        }
    }
}
