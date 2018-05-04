package com.skynyrd.kafka.transform;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.skynyrd.kafka.model.SinkOp;
import com.skynyrd.kafka.model.SinkPayload;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Collections;
import java.util.Optional;

public abstract class AbstractRecordTransformer implements RecordTransformer {
    private static Logger log = LogManager.getLogger(AbstractRecordTransformer.class);

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


            String opStr = recordAsJson.getAsJsonObject("payload").get("op").getAsString();
            SinkOp op = SinkOp.fromStr(opStr);

            Optional<JsonObject> after = Optional.empty();
            try {
                after = Optional.of(recordAsJson.getAsJsonObject("payload").getAsJsonObject("after"));
            } catch (Exception e) {
                log.error(e);
            }

            Optional<JsonObject> before = Optional.empty();
            try {
                before = Optional.of(recordAsJson.getAsJsonObject("payload").getAsJsonObject("before"));
            } catch (Exception e) {
                log.error(e);
            }

            return new SinkPayload(op, before, after);
        } catch (Exception e) {
            throw new ParseException("Error parsing record + " + record, -1);
        }
    }
}
