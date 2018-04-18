package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.model.RecordType;
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
        Optional<JsonObject> payloadOpt = extractPayload(record).getAfter();

        if (!payloadOpt.isPresent()) {
            return Optional.empty();
        }

        JsonObject payload = payloadOpt.get();

        try {
            String id = payload.get("id").getAsString();

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

            JsonElement prodCatJson = payload.get("country");
            if (prodCatJson == null || prodCatJson.isJsonNull()) {
                docJson.add(
                        "product_categories",
                        new JsonArray()
                );
            } else {
                docJson.add(
                        "product_categories",
                        gson.fromJson(prodCatJson, JsonArray.class)
                );
            }

            docJson.add("suggest",
                    Utils.createLocalSuggestions(
                            gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
                    )
            );

            return Optional.of(new Record(docJson, id, RecordType.INSERT));
        } catch (Exception e) {
            LOG.error("Error parsing payload [" + payload + "]");
            LOG.error(e);
            throw new ParseException("Error parsing payload", -1);
        }
    }
}
