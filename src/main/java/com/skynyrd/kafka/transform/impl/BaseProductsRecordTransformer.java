package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.model.RecordType;
import com.skynyrd.kafka.transform.AbstractRecordTransformer;
import com.skynyrd.kafka.transform.Utils;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.ParseException;

public class BaseProductsRecordTransformer extends AbstractRecordTransformer {

    @Override
    public Record apply(SinkRecord record) throws ParseException {
        JsonObject payload = extractPayload(record);
        String id = payload.get("id").getAsString();

        JsonObject docJson = new JsonObject();
        docJson.addProperty("id", payload.get("id").getAsLong());
        docJson.addProperty("category_id", payload.get("category_id").getAsLong());
        docJson.add(
                "name",
                gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
        );
        docJson.add(
                "short_description",
                gson.fromJson(payload.get("short_description").getAsString(), JsonArray.class)
        );
        if (!payload.get("long_description").isJsonNull()) {
            docJson.add(
                    "long_description",
                    gson.fromJson(payload.get("long_description").getAsString(), JsonArray.class)
            );
        }
        docJson.add("variants", new JsonArray());

        docJson.add("suggest",
                Utils.createLocalSuggestions(
                        gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
                )
        );

        return new Record(docJson, id, RecordType.INSERT);
    }


}
