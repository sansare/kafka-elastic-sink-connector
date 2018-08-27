package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.skynyrd.kafka.Consts;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.model.RecordType;
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
        Optional<JsonObject> after = sinkPayload.getAfter();
        Optional<JsonObject> before = sinkPayload.getBefore();

        switch (sinkPayload.getOp()) {
            case CREATE:
                return after.map(this::createInsertRecord);
            case UPDATE:
                return after.map(this::createUpdateRecord);
            case DELETE:
                return before.map(this::createDeleteRecord);
            case DB_SOFT_DELETE:
                return after.map(this::createDeleteRecord);
            default:
                return Optional.empty();
        }
    }

    private Record createDeleteRecord(JsonObject payload) {
        String id = payload.get("id").getAsString();
        return new Record(new JsonObject(), id, RecordType.DELETE, Consts.PRODUCTS_INDEX);
    }

    private Record createInsertRecord(JsonObject payload) {
        String id = payload.get("id").getAsString();

        JsonObject docJson = new JsonObject();

        docJson.addProperty("id", payload.get("id").getAsLong());
        docJson.addProperty("category_id", payload.get("category_id").getAsLong());
        docJson.addProperty("currency", payload.get("currency").getAsString());
        docJson.addProperty("store_id", payload.get("store_id").getAsLong());

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
        docJson.addProperty("status", payload.get("status").getAsString());

        docJson.add("suggest_2",
                Utils.createProductSuggestions(
                        gson.fromJson(payload.get("name").getAsString(), JsonArray.class),
                        payload.get("store_id").getAsLong(),
                        payload.get("status").getAsString()
                )
        );

        return new Record(docJson, id, RecordType.INSERT, Consts.PRODUCTS_INDEX);
    }

    private Record createUpdateRecord(JsonObject payload) {
        String id = payload.get("id").getAsString();

        String updScript =
                "ctx._source.category_id = params.category_id;" +
                "ctx._source.currency = params.currency;" +
                "ctx._source.store_id = params.store_id;" +
                "ctx._source.name = params.name;" +
                "ctx._source.short_description = params.short_description;" +
                "ctx._source.long_description = params.long_description;" +
                "ctx._source.views = params.views;" +
                "ctx._source.rating = params.rating;" +
                "ctx._source.status = params.status;" +
                "ctx._source.suggest_2 = params.suggest_2;";

        JsonObject docJson = new JsonObject();

        JsonObject scriptJson = new JsonObject();
        scriptJson.addProperty("source", updScript);

        JsonObject paramsObj = new JsonObject();

        paramsObj.addProperty("category_id", payload.get("category_id").getAsLong());
        paramsObj.addProperty("currency", payload.get("currency").getAsString());
        paramsObj.addProperty("store_id", payload.get("store_id").getAsLong());

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
        paramsObj.addProperty("rating", payload.get("rating").getAsDouble());
        paramsObj.addProperty("status", payload.get("status").getAsString());

        paramsObj.add("suggest_2",
                Utils.createProductSuggestions(
                        gson.fromJson(payload.get("name").getAsString(), JsonArray.class),
                        payload.get("store_id").getAsLong(),
                        payload.get("status").getAsString()
                )
        );

        scriptJson.add("params", paramsObj);

        docJson.add("script", scriptJson);

        return new Record(docJson, id, RecordType.UPDATE, Consts.PRODUCTS_INDEX);
    }
}
