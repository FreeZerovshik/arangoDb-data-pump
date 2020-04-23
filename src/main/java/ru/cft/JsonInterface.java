package main.java.ru.cft;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public interface JsonInterface {
    @NotNull
    static String toJson(@NotNull Object object) {
        try {
            return JsonHelper.mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    static <T> T fromJson(@NotNull String json, @NotNull Class<T> type) {
        try {
            return JsonHelper.mapper.readValue(json, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    static JsonNode getJsonNode(@NotNull String json) {
        return fromJson(json, JsonNode.class);
    }


}
