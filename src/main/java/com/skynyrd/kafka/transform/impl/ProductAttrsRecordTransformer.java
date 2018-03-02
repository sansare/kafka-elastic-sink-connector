package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonObject;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.model.RecordType;
import com.skynyrd.kafka.transform.AbstractRecordTransformer;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.ParseException;

public class ProductAttrsRecordTransformer extends AbstractRecordTransformer {

    @Override
    public Record apply(SinkRecord record) throws ParseException {
        JsonObject payload = extractPayload(record);
        String id = payload.get("prod_id").getAsString();

        String updScript =
                "boolean updated = false; " +
                "for (int i = 0; i < ctx._source.attrs.length; i++) {" +
                "  if (ctx._source.attrs[i].attr_id == params.attr.attr_id) {" +
                "    ctx._source.attrs[i] = params.attr; updated = true; break;" +
                "  }" +
                "}" +
                "if (updated == false) {" +
                "  ctx._source.attrs.add(params.attr);" +
                "}";

        JsonObject docJson = new JsonObject();

        JsonObject scriptJson = new JsonObject();
        scriptJson.addProperty("source", updScript);
        scriptJson.add("params", createAttrWrapper(payload));

        docJson.add("script", scriptJson);

        return new Record(docJson, id, RecordType.UPDATE);
    }

    private JsonObject createAttrWrapper(JsonObject payload) throws ParseException {
        try {
            JsonObject attrObj = new JsonObject();
            attrObj.addProperty("attr_id", payload.get("attr_id").getAsLong());

            String type = payload.get("value_type").getAsString();
            String value = payload.get("value").getAsString();

            switch (type) {
                case "Float":
                    attrObj.addProperty("float_val", Double.parseDouble(value));
                    break;
                default:
                    attrObj.addProperty("str_val", value);
            }

            JsonObject attrWrapper = new JsonObject();
            attrWrapper.add("attr", attrObj);

            return attrWrapper;
        } catch (Exception e) {
            throw new ParseException("Error extracting attribute", -1);
        }
    }
}
