package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonObject;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.model.RecordType;
import com.skynyrd.kafka.transform.AbstractRecordTransformer;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.ParseException;

public class StoresRecordTransformer extends AbstractRecordTransformer {

    @Override
    public Record apply(SinkRecord record) throws ParseException {
        JsonObject payload = extractPayload(record);
        String id = payload.get("id").getAsString();

        JsonObject docJson = new JsonObject();
        docJson.addProperty("id", payload.get("id").getAsLong());
        docJson.addProperty("user_id", payload.get("user_id").getAsLong());
        docJson.add("name", payload.get("name").getAsJsonArray());

        return new Record(docJson, id, RecordType.INSERT);
    }
}
