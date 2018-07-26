package com.skynyrd.kafka.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Utils {

    public static JsonArray createStoreSuggestions(JsonArray translations) {
        JsonArray suggestions = new JsonArray();

        for (JsonElement element : translations) {
            if (element.isJsonObject()) {
                JsonElement textElement = element.getAsJsonObject().get("text");
                if (!textElement.isJsonNull()) {
                    for (String token : textElement.getAsString().split("\\s+")) {
                        suggestions.add(token);
                    }
                    suggestions.add(textElement.getAsString());
                }
            }
        }

        return suggestions;
    }

    public static JsonObject createProductSuggestions(JsonArray translations, Long store_id) {
        JsonObject suggestions = new JsonObject();
        JsonArray input = new JsonArray();

        for (JsonElement element : translations) {
            if (element.isJsonObject()) {
                JsonElement textElement = element.getAsJsonObject().get("text");
                if (!textElement.isJsonNull()) {
                    for (String token : textElement.getAsString().split("\\s+")) {
                        input.add(token);
                    }
                    input.add(textElement.getAsString());
                }
            }
        }
        suggestions.add("input",input);

        JsonArray store = new JsonArray();
        store.add(store_id.toString());

        JsonObject contexts = new JsonObject();
        contexts.add("store", store);

        suggestions.add("contexts",contexts);

        return suggestions;
    }
}
