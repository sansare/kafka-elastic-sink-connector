package com.skynyrd.kafka.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

public class Utils {

    public static JsonArray createLocalSuggestions(JsonArray translations) {
        JsonArray suggestions = new JsonArray();

        for (JsonElement element : translations) {
            if (element.isJsonObject()) {
                JsonElement textElement = element.getAsJsonObject().get("text");
                if (!textElement.isJsonNull()) {
                    for (String token : textElement.getAsString().split("\\s+")) {
                        suggestions.add(token);
                    }
                }
            }
        }

        return suggestions;
    }
}
