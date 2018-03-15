package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.model.RecordType;
import com.skynyrd.kafka.transform.AbstractRecordTransformer;
import com.skynyrd.kafka.transform.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;

public class StoresRecordTransformer extends AbstractRecordTransformer {
    private static Logger LOG = LogManager.getLogger(StoresRecordTransformer.class);

    @Override
    public Record apply(SinkRecord record) throws ParseException {
        JsonObject payload = extractPayload(record);

        try {
            String id = payload.get("id").getAsString();

            JsonObject docJson = new JsonObject();
            docJson.addProperty("id", payload.get("id").getAsLong());
            docJson.addProperty("user_id", payload.get("user_id").getAsLong());
            docJson.add(
                    "name",
                    gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
            );

            docJson.add("suggest",
                    Utils.createLocalSuggestions(
                            gson.fromJson(payload.get("name").getAsString(), JsonArray.class)
                    )
            );

            return new Record(docJson, id, RecordType.INSERT);
        } catch (Exception e) {
            LOG.error("Error parsing payload [" + payload);
            throw new ParseException("Error parsing payload", -1);
        }
    }
}
