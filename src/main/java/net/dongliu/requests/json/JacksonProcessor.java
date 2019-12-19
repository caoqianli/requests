package net.dongliu.requests.json;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.Charset;

/**
 * Provider json ability via jackson
 *
 * @author Liu Dong
 */
public class JacksonProcessor implements JsonProcessor {

    private final ObjectMapper objectMapper;

    public JacksonProcessor() {
        this(createDefault());
    }

    private static ObjectMapper createDefault() {
        return new ObjectMapper().findAndRegisterModules();
    }

    public JacksonProcessor(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void marshal(Writer writer, @Nullable Object value) throws IOException {
        objectMapper.writeValue(writer, value);
    }

    @Override
    public <T> T unmarshal(InputStream inputStream, Charset charset, Type type) throws IOException {
        try (Reader reader = new InputStreamReader(inputStream, charset)) {
            JavaType javaType = objectMapper.getTypeFactory().constructType(type);
            return objectMapper.readValue(reader, javaType);
        }
    }
}
