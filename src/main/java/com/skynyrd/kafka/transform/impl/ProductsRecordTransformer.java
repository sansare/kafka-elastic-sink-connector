package com.skynyrd.kafka.transform.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.skynyrd.kafka.Consts;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.model.RecordType;
import com.skynyrd.kafka.model.SinkPayload;
import com.skynyrd.kafka.transform.AbstractRecordTransformer;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.ParseException;
import java.util.Optional;

public class ProductsRecordTransformer extends AbstractRecordTransformer {

    @Override
    public Optional<Record> apply(SinkRecord record) throws ParseException {
        SinkPayload sinkPayload = extractPayload(record);
        Optional<JsonObject> payload = sinkPayload.getPayload();

        if (!payload.isPresent()) {
            return Optional.empty();
        }

        switch (sinkPayload.getOp()) {
            case CREATE:
            case UPDATE:
                return Optional.of(createRecord(payload.get()));
            default:
                return Optional.empty();
        }
    }

    private Record createRecord(JsonObject payload) throws ParseException {
        String id = payload.get("base_product_id").getAsString();

        String updScript =
                "boolean updated = false;" +
                "def vars = ctx._source.variants;" +
                "def var_param = params.variant;" +
                "for (int i = 0; i < vars.length; i++) {" +
                "    if (vars[i].prod_id == var_param.prod_id) {" +
                "        vars[i].price = var_param.price;" +
                "        vars[i].discount = var_param.discount;" +
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

        return new Record(docJson, id, RecordType.UPDATE, Consts.PRODUCTS_INDEX);
    }

    private JsonObject createVariantWrapper(JsonObject payload) throws ParseException {
        try {
            JsonObject variantWrapper = new JsonObject();
            variantWrapper.add("variant", createVariantObj(payload));

            return variantWrapper;
        } catch (Exception e) {
            throw new ParseException("Error extracting attribute: " + e.getLocalizedMessage(), -1);
        }
    }

    private JsonObject createVariantObj(JsonObject payload) {
        JsonObject variantObj = new JsonObject();
        variantObj.addProperty("prod_id", payload.get("id").getAsLong());

        variantObj.addProperty("price", payload.get("price").getAsDouble());

        JsonElement discount = payload.get("discount");
        if (!discount.isJsonNull()) {
            variantObj.addProperty("discount", discount.getAsDouble());
        }

        variantObj.add("attrs", new JsonArray());

        return variantObj;
    }
}
