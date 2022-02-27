package com.skynyrd.kafka.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Utils {

    public static JsonObject createStoreSuggestions(JsonArray translations, String moderation_status) {
        JsonArray input = new JsonArray();

        for (JsonElement element : translations) {
            if (element.isJsonObject()) {
                JsonElement textElement = element.getAsJsonObject().get("text");
                if (!textElement.isJsonNull()) {
                    input.add(textElement.getAsString());
                    for (String token : textElement.getAsString().split("\\s+")) {
                        input.add(token);
                    }
                }
            }
        }

        JsonArray status = new JsonArray();
        status.add(moderation_status);

        JsonObject contexts = new JsonObject();
        contexts.add("status", status);

        JsonObject suggestions = new JsonObject();
        suggestions.add("input", input);
        suggestions.add("contexts", contexts);

        return suggestions;
    }

    public static JsonObject createProductSuggestions(JsonArray translations, Long store_id, String moderation_status) {
        JsonArray input = new JsonArray();

        for (JsonElement element : translations) {
            if (element.isJsonObject()) {
                JsonElement textElement = element.getAsJsonObject().get("text");
                if (!textElement.isJsonNull()) {
                    input.add(textElement.getAsString());
                    for (String token : textElement.getAsString().split("\\s+")) {
                        input.add(token);
                    }
                }
            }
        }

        JsonArray store_and_status = new JsonArray();
        //workaround because elastic doesn't afford to ANY contexts
        store_and_status.add(store_id.toString() + "_" + moderation_status);
        //workaround because context also need to filter on published status
        store_and_status.add(moderation_status);

        JsonObject contexts = new JsonObject();
        contexts.add("store_and_status", store_and_status);

        JsonObject suggestions = new JsonObject();
        suggestions.add("input", input);
        suggestions.add("contexts", contexts);

        return suggestions;
    }
}
