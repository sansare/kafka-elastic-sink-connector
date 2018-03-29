package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.model.RecordType;
import com.skynyrd.kafka.model.SinkOp;
import com.skynyrd.kafka.model.SinkPayload;
import com.skynyrd.kafka.transform.AbstractRecordTransformer;
import com.skynyrd.kafka.transform.Utils;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.ParseException;
import java.util.Optional;

public class BaseProductsRecordTransformer extends AbstractRecordTransformer {

    @Override
    public Optional<Record> apply(SinkRecord record) throws ParseException {
        SinkPayload payload = extractPayload(record);
        Optional<JsonObject> afterPayload = payload.getAfter();

        if (!afterPayload.isPresent()) {
            return Optional.empty();
        }

        if (payload.getOp() == SinkOp.UPDATE) {
            return Optional.of(createUpdateRecord(afterPayload.get()));
        } else {
            return Optional.of(createInsertRecord(afterPayload.get()));
        }
    }

    private Record createInsertRecord(JsonObject payload) {
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

        docJson.addProperty("views", payload.get("views").getAsLong());

        docJson.add("suggest",
                Utils.createLocalSuggestions(
                        gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
                )
        );

        return new Record(docJson, id, RecordType.INSERT);
    }

    private Record createUpdateRecord(JsonObject payload) {
        String id = payload.get("id").getAsString();
        long views = payload.get("views").getAsLong();

        String updScript =
                "ctx._source.views = params.views";

        JsonObject docJson = new JsonObject();

        JsonObject scriptJson = new JsonObject();
        scriptJson.addProperty("source", updScript);

        JsonObject viewsObj = new JsonObject();
        viewsObj.addProperty("views", views);

        scriptJson.add("params", viewsObj);

        docJson.add("script", scriptJson);

        return new Record(docJson, id, RecordType.UPDATE);
    }
}
