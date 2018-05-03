package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.skynyrd.kafka.Consts;
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
        SinkPayload sinkPayload = extractPayload(record);
        Optional<JsonObject> payload = sinkPayload.getPayload();

        if (!payload.isPresent()) {
            return Optional.empty();
        }

        switch (sinkPayload.getOp()) {
            case CREATE:
                return Optional.of(createInsertRecord(payload.get()));
            case UPDATE:
                return Optional.of(createUpdateRecord(payload.get()));
            default:
                return Optional.empty();
        }
    }

    private Record createInsertRecord(JsonObject payload) {
        String id = payload.get("id").getAsString();

        JsonObject docJson = new JsonObject();
        docJson.addProperty("id", payload.get("id").getAsLong());
        docJson.addProperty("category_id", payload.get("category_id").getAsLong());
        docJson.addProperty("currency_id", payload.get("currency_id").getAsLong());
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

        docJson.addProperty("rating", payload.get("rating").getAsLong());

        docJson.add("suggest",
                Utils.createLocalSuggestions(
                        gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
                )
        );

        return new Record(docJson, id, RecordType.INSERT, Consts.PRODUCTS_INDEX);
    }

    private Record createUpdateRecord(JsonObject payload) {
        String id = payload.get("id").getAsString();

        String updScript =
                "ctx._source.category_id = params.category_id;" +
                "ctx._source.currency_id = params.currency_id;" +
                "ctx._source.name = params.name;" +
                "ctx._source.short_description = params.short_description;" +
                "ctx._source.long_description = params.long_description;" +
                "ctx._source.views = params.views;" +
                "ctx._source.rating = params.rating;";

        JsonObject docJson = new JsonObject();

        JsonObject scriptJson = new JsonObject();
        scriptJson.addProperty("source", updScript);

        JsonObject paramsObj = new JsonObject();

        paramsObj.addProperty("category_id", payload.get("category_id").getAsLong());
        paramsObj.addProperty("currency_id", payload.get("currency_id").getAsLong());
        paramsObj.add(
                "name",
                gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
        );
        paramsObj.add(
                "short_description",
                gson.fromJson(payload.get("short_description").getAsString(), JsonArray.class)
        );
        if (!payload.get("long_description").isJsonNull()) {
            paramsObj.add(
                    "long_description",
                    gson.fromJson(payload.get("long_description").getAsString(), JsonArray.class)
            );
        }

        paramsObj.addProperty("views", payload.get("views").getAsLong());
        paramsObj.addProperty("rating", payload.get("rating").getAsLong());

        scriptJson.add("params", paramsObj);

        docJson.add("script", scriptJson);

        return new Record(docJson, id, RecordType.UPDATE, Consts.PRODUCTS_INDEX);
    }
}
