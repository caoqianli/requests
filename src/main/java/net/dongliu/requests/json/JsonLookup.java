package net.dongliu.requests.json;



import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Lookup json, from classpath
 *
 * @author Liu Dong
 */
public class JsonLookup {
    private static JsonLookup instance = new JsonLookup();
    @Nullable
    private volatile JsonProcessor registeredJsonProcessor;

    private JsonLookup() {
    }

    public static JsonLookup getInstance() {
        return instance;
    }

    /**
     * Set json provider for using.
     *
     * @see JsonProcessor
     * @see JacksonProcessor
     * @see GsonProcessor
     */
    public void register(JsonProcessor jsonProcessor) {
        this.registeredJsonProcessor = Objects.requireNonNull(jsonProcessor);
    }

    /**
     * If classpath has gson
     */
    boolean hasGson() {
        try {
            Class.forName("com.google.gson.Gson");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Create Gson Provider
     */
    JsonProcessor gsonProvider() {
        return new GsonProcessor();
    }

    /**
     * if jackson in classpath
     */
    boolean hasJackson() {
        try {
            Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    boolean hasFastJson() {
        try {
            Class.forName("com.alibaba.fastjson.JSON");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Create Jackson Provider
     */
    JsonProcessor jacksonProvider() {
        return new JacksonProcessor();
    }

    JsonProcessor fastJsonProvider() {
        return new FastJsonProcessor();
    }

    /**
     * Find one json provider.
     *
     * @throws JsonProcessorNotFoundException if no json provider found
     */
    @NonNull
    public JsonProcessor lookup() {
        JsonProcessor registeredJsonProcessor = this.registeredJsonProcessor;
        if (registeredJsonProcessor != null) {
            return registeredJsonProcessor;
        }

        if (!init) {
            synchronized (this) {
                if (!init) {
                    lookedJsonProcessor = lookupInClasspath();
                    init = true;
                }
            }
        }

        if (lookedJsonProcessor != null) {
            return lookedJsonProcessor;
        }
        throw new JsonProcessorNotFoundException("Json Provider not found");
    }

    @Nullable
    private JsonProcessor lookedJsonProcessor;
    private boolean init;

    @Nullable
    private JsonProcessor lookupInClasspath() {
        if (hasJackson()) {
            return jacksonProvider();
        }
        if (hasGson()) {
            return gsonProvider();
        }
        if (hasFastJson()) {
            return fastJsonProvider();
        }
        return null;
    }
}
