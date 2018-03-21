package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.model.RecordType;
import com.skynyrd.kafka.model.SinkPayload;
import com.skynyrd.kafka.transform.AbstractRecordTransformer;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.ParseException;
import java.util.Optional;

public class ProdAttrsRecordTransformer extends AbstractRecordTransformer {

    @Override
    public Optional<Record> apply(SinkRecord record) throws ParseException {
        Optional<JsonObject> payload = extractPayload(record).getAfter();

        if (!payload.isPresent()) {
            return Optional.empty();
        }

        JsonObject afterPayload = payload.get();

        return Optional.of(createRecord(afterPayload));
    }

    private Record createRecord(JsonObject payload) throws ParseException {
        String id = payload.get("base_prod_id").getAsString();

        String updScript =
                "boolean updated = false;" +
                "def vars = ctx._source.variants;" +
                "def var_param = params.variant;" +
                "for (int i = 0; i < vars.length; i++) {" +
                "    if (vars[i].prod_id == var_param.prod_id) {" +

                "        def attrs_param = var_param.attrs;" +
                "        for (int j = 0; j < attrs_param.length; j++) {" +

                "            boolean attrs_updated = false;" +
                "            for (int k = 0; k < vars[i].attrs.length; k++) {" +
                "                if (vars[i].attrs[k].attr_id == attrs_param[j].attr_id) {" +
                "                    vars[i].attrs[k] = attrs_param[j];" +
                "                    attrs_updated = true;" +
                "                    break;" +
                "                }" +
                "            }" +
                "            if (attrs_updated == false) {" +
                "                vars[i].attrs.add(attrs_param[j]);" +
                "            }" +
                "        }" +

                "        updated = true;" +
                "        break;" +
                "    }" +
                "}" +
                "if (updated == false) {" +
                "    ctx._source.variants.add(var_param);" +
                "}";

        JsonObject docJson = new JsonObject();

        JsonObject scriptJson = new JsonObject();
        scriptJson.addProperty("source", updScript);
        scriptJson.add("params", createVariantWrapper(payload));

        docJson.add("script", scriptJson);

        return new Record(docJson, id, RecordType.UPDATE);
    }

    private JsonObject createVariantWrapper(JsonObject payload) throws ParseException {
        try {
            JsonObject variantWrapper = new JsonObject();
            variantWrapper.add("variant", createVariantObj(payload));

            return variantWrapper;
        } catch (Exception e) {
            throw new ParseException("Error extracting attribute", -1);
        }
    }

    private JsonObject createVariantObj(JsonObject payload) {
        JsonObject variantObj = new JsonObject();
        variantObj.addProperty("prod_id", payload.get("prod_id").getAsLong());
        variantObj.addProperty("discount", 0.0);
        variantObj.add("attrs", createAttrsArr(payload));

        return variantObj;
    }

    private JsonArray createAttrsArr(JsonObject payload) {
        JsonObject attrObj = new JsonObject();
        attrObj.addProperty("attr_id", payload.get("attr_id").getAsLong());

        String type = payload.get("value_type").getAsString().toLowerCase();
        String value = payload.get("value").getAsString();

        switch (type) {
            case "float":
                attrObj.addProperty("float_val", Double.parseDouble(value));
                break;
            default:
                attrObj.addProperty("str_val", value);
        }

        JsonArray attrsArr = new JsonArray();
        attrsArr.add(attrObj);

        return attrsArr;
    }
}
