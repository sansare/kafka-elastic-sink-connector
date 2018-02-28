package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonObject;
import com.skynyrd.kafka.Record;
import com.skynyrd.kafka.transform.AbstractRecordTransformer;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.json.Json;
import java.text.ParseException;

public class ProductsRecordTransformer extends AbstractRecordTransformer {

    @Override
    public Record apply(SinkRecord record) throws ParseException {
        JsonObject payload = extractPayload(record);
        String id = payload.get("id").getAsString();


        javax.json.JsonObject docJson = Json.createObjectBuilder()
                .add("id", payload.get("id").getAsString())
                .add("store_id", payload.get("store_id").getAsString())
                .add("name", payload.get("name").getAsString())
                .add("attrs", Json.createArrayBuilder().build())
                .build();

        return new Record(docJson, id);
    }
}
