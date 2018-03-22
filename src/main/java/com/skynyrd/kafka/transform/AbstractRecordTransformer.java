package com.skynyrd.kafka.transform;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.skynyrd.kafka.model.SinkPayload;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Collections;
import java.util.Optional;

public abstract class AbstractRecordTransformer implements RecordTransformer {
    protected JsonConverter jsonConverter;
    protected Gson gson;

    public AbstractRecordTransformer() {
        jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap("schemas", false), false);

        gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                .create();
    }

    protected SinkPayload extractPayload(SinkRecord record) throws ParseException {
        try {
            byte[] rawJsonPayload = jsonConverter.fromConnectData(
                    record.topic(), record.valueSchema(), record.value());
            String recordStr = new String(rawJsonPayload, StandardCharsets.UTF_8);
            JsonObject recordAsJson = gson.fromJson(recordStr, JsonObject.class);

            Optional<JsonObject> before = Optional.empty();
            try {
                 before = Optional.of(recordAsJson.getAsJsonObject("payload").getAsJsonObject("before"));
            } catch (Exception e) {
                // ignored
            }

            Optional<JsonObject> after = Optional.empty();
            try {
                after = Optional.of(recordAsJson.getAsJsonObject("payload").getAsJsonObject("after"));
            } catch (Exception e) {
                // ignored
            }
            return new SinkPayload(before, after);
        } catch (Exception e) {
            throw new ParseException("Error parsing record + " + record, -1);
        }
    }
}
