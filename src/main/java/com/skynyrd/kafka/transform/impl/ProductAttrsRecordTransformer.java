package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonObject;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.model.RecordType;
import com.skynyrd.kafka.transform.AbstractRecordTransformer;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.json.Json;
import java.text.ParseException;

public class ProductAttrsRecordTransformer extends AbstractRecordTransformer {

    @Override
    public Record apply(SinkRecord record) throws ParseException {
        JsonObject payload = extractPayload(record);
        String id = payload.get("prod_id").getAsString();

        javax.json.JsonObject docJson = Json.createObjectBuilder()
                .add("id", payload.get("prod_id").getAsString())
                .add("attrs",
                        Json.createArrayBuilder().add(
                                createAttrObj(payload)
                        ))
                .build();

        javax.json.JsonObject partialUpdWrapper = Json.createObjectBuilder()
                .add("script", "ctx._source.attrs += attr")
                .add("params",
                        Json.createObjectBuilder()
                                .add("attr",
                                        Json.createArrayBuilder()
                                        .add(docJson)
                                        .build()
                                ).build()
                )
                .build();

        return new Record(partialUpdWrapper, id, RecordType.UPDATE);
    }

    private javax.json.JsonObject createAttrObj(JsonObject payload) throws ParseException {
        try {
            javax.json.JsonObjectBuilder attrObjBuilder = Json.createObjectBuilder();
            attrObjBuilder.add("attr_id", payload.get("attr_id").getAsString());

            String type = payload.get("value_type").getAsString();
            String value = payload.get("value").getAsString();

            switch (type) {
                case "Float":
                    attrObjBuilder.add("float_val", Double.parseDouble(value));
                    break;
                default:
                    attrObjBuilder.add("str_val", value);
            }

            return attrObjBuilder.build();
        } catch (Exception e) {
            throw new ParseException("Error extracting attribute", -1);
        }
    }
}
