package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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
        Optional<JsonObject> payload = sinkPayload.getPayload();

        if (!payload.isPresent()) {
            return Optional.empty();
        }

        switch (sinkPayload.getOp()) {
            case CREATE:
                return Optional.of(createRecord(payload.get(), RecordType.INSERT));
            case UPDATE:
                return Optional.of(createRecord(payload.get(), RecordType.UPDATE));
            default:
                return Optional.empty();
        }
    }

    private Record createRecord(JsonObject payload, RecordType recordType) throws ParseException {
        try {
            String id = payload.get("id").getAsString();

            JsonObject docJson = new JsonObject();
            docJson.addProperty("id", payload.get("id").getAsLong());
            docJson.addProperty("user_id", payload.get("user_id").getAsLong());
            docJson.add(
                    "name",
                    gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
            );
            docJson.addProperty("country", payload.get("country").getAsString());
            docJson.addProperty("rating", payload.get("rating").getAsLong());
            docJson.add(
                    "product_categories",
                    gson.fromJson(payload.get("product_categories").getAsString(), JsonArray.class)
            );

            docJson.add("suggest",
                    Utils.createLocalSuggestions(
                            gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
                    )
            );

            return new Record(docJson, id, recordType);
        } catch (Exception e) {
            LOG.error("Error parsing payload [" + payload);
            throw new ParseException("Error parsing payload", -1);
        }
    }
}
