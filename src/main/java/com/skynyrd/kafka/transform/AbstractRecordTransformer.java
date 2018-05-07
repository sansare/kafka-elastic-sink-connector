package com.skynyrd.kafka.transform;

import com.google.gson.*;
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

            JsonElement payloadElem = recordAsJson.get("payload");
            if (payloadElem == null || payloadElem.isJsonNull()) {
                return new SinkPayload(SinkOp.UNKNOWN, Optional.empty(), Optional.empty());
            }
            String opStr = payloadElem.getAsJsonObject().get("op").getAsString();
            SinkOp op = SinkOp.fromStr(opStr);

            Optional<JsonObject> before = Optional.empty();
            try {
                JsonElement beforeElem = recordAsJson.getAsJsonObject("payload").get("before");
                if (beforeElem != null && !beforeElem.isJsonNull()) {
                    before = Optional.of(beforeElem.getAsJsonObject());
                }
            } catch (Exception e) {
                log.error(e);
            }

            Optional<JsonObject> after = Optional.empty();
            try {
                JsonElement afterElem = recordAsJson.getAsJsonObject("payload").get("after");
                if (afterElem != null && !afterElem.isJsonNull()) {
                    after = Optional.of(afterElem.getAsJsonObject());
                }
            } catch (Exception e) {
                log.error(e);
            }

            return new SinkPayload(op, before, after);
        } catch (Exception e) {
            throw new ParseException("Error parsing record + " + record, -1);
        }
    }
}
