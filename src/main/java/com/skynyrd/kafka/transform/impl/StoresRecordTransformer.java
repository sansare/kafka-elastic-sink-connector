package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.skynyrd.kafka.Consts;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.model.RecordType;
import com.skynyrd.kafka.model.SinkPayload;
import com.skynyrd.kafka.transform.AbstractRecordTransformer;
import com.skynyrd.kafka.transform.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.util.Optional;

public class StoresRecordTransformer extends AbstractRecordTransformer {
    private static Logger LOG = LogManager.getLogger(StoresRecordTransformer.class);

    @Override
    public Optional<Record> apply(SinkRecord record) throws ParseException {
        SinkPayload sinkPayload = extractPayload(record);
        Optional<JsonObject> after = sinkPayload.getAfter();
        Optional<JsonObject> before = sinkPayload.getAfter();

        switch (sinkPayload.getOp()) {
            case CREATE:
                if (after.isPresent()) {
                    return Optional.of(createInsertRecord(after.get()));
                } else {
                    return Optional.empty();
                }
            case UPDATE:
                if (after.isPresent()) {
                    return Optional.of(createUpdateRecord(after.get()));
                } else {
                    return Optional.empty();
                }
            case DELETE:
                return before.map(this::createDeleteRecord);
            default:
                return Optional.empty();
        }
    }

    private Record createDeleteRecord(JsonObject payload) {
        String id = payload.get("id").getAsString();
        return new Record(new JsonObject(), id, RecordType.DELETE, Consts.PRODUCTS_INDEX);
    }

    private JsonObject createDoc(JsonObject payload) {
        JsonObject docJson = new JsonObject();
        docJson.addProperty("id", payload.get("id").getAsLong());
        docJson.addProperty("user_id", payload.get("user_id").getAsLong());
        docJson.add(
                "name",
                gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
        );

        JsonElement countryJson = payload.get("country");
        String country = countryJson == null || countryJson.isJsonNull()
                ? ""
                : countryJson.getAsString();
        docJson.addProperty("country", country);

        docJson.addProperty("rating", payload.get("rating").getAsLong());

        JsonElement prodCatJson = payload.get("product_categories");
        if (prodCatJson == null || prodCatJson.isJsonNull()) {
            docJson.add(
                    "product_categories",
                    new JsonArray()
            );
        } else {
            docJson.add(
                    "product_categories",
                    gson.fromJson(prodCatJson.getAsString(), JsonArray.class)
            );
        }

        docJson.add("suggest",
                Utils.createLocalSuggestions(
                        gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
                )
        );

        return docJson;
    }

    private Record createInsertRecord(JsonObject payload) throws ParseException {
        try {
            String id = payload.get("id").getAsString();
            JsonObject docJson = createDoc(payload);
            return new Record(docJson, id, RecordType.INSERT, Consts.STORES_INDEX);
        } catch (Exception e) {
            LOG.error("Error parsing payload [" + payload);
            throw new ParseException("Error parsing payload", -1);
        }
    }

    private Record createUpdateRecord(JsonObject payload) throws ParseException {
        try {
            String id = payload.get("id").getAsString();

            String updScript =
                    "ctx._source = params.payload;";

            JsonObject docJson = new JsonObject();

            JsonObject scriptJson = new JsonObject();
            scriptJson.addProperty("source", updScript);

            JsonObject paramsObj = new JsonObject();
            paramsObj.add("payload",  createDoc(payload));

            scriptJson.add("params", paramsObj);

            docJson.add("script", scriptJson);

            return new Record(docJson, id, RecordType.UPDATE, Consts.STORES_INDEX);
        } catch (Exception e) {
            LOG.error("Error parsing payload [" + payload);
            throw new ParseException("Error parsing payload", -1);
        }
    }
}
