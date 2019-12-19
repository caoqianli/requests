package net.dongliu.requests;


import net.dongliu.commons.Lazy;
import net.dongliu.commons.collection.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * Wrap to deal with response headers.
 * This class is thread-safe.
 *
 * @author Liu Dong
 */
public class Headers implements Serializable {
    private static final long serialVersionUID = -1283402589869346874L;
    private final List<Header> headers;
    private transient final Lazy<Map<String, List<String>>> lazyMap;

    public Headers(List<Header> headers) {
        this.headers = Lists.copy(requireNonNull(headers));
        this.lazyMap = Lazy.of(() -> toMap(headers));
    }

    private static Map<String, List<String>> toMap(List<Header> headers) {
        Map<String, List<String>> map = new HashMap<>();
        for (Map.Entry<String, String> header : headers) {
            String key = header.getKey().toLowerCase();
            String value = header.getValue();
            List<String> list = map.get(key);
            if (list == null) {
                list = new ArrayList<>(4);
                list.add(value);
                map.put(key, list);
            } else {
                list.add(value);
            }
        }
        return map;
    }

    /**
     * Get headers by name. If not exists, return empty list
     */
    public List<String> getHeaders(String name) {
        requireNonNull(name);
        List<String> values = lazyMap.get().get(name.toLowerCase());
        if (values == null) {
            return Lists.of();
        }
        return Collections.unmodifiableList(values);
    }

    /**
     * Get the first header value matched name. If not exists, return null.
     *
     * @deprecated using {@link #getHeader(String)} instead
     */
    @Deprecated
    @Nullable
    public String getFirstHeader(String name) {
        requireNonNull(name);
        return getHeader(name);
    }

    /**
     * Get the first header value matched name. If not exists, return null.
     */
    @Nullable
    public String getHeader(String name) {
        requireNonNull(name);
        List<String> values = lazyMap.get().get(name.toLowerCase());
        if (values == null) {
            return null;
        }
        return values.get(0);
    }

    /**
     * Get header value as long. If not exists, return defaultValue
     */
    public long getLongHeader(String name, long defaultValue) {
        String firstHeader = getHeader(name);
        if (firstHeader == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(firstHeader.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public List<Header> getHeaders() {
        return headers;
    }

    /**
     * Get charset set in content type header.
     *
     * @return the charset, or defaultCharset if no charset is set.
     */
    public Charset getCharset(Charset defaultCharset) {
        String contentType = getHeader(HttpHeaders.NAME_CONTENT_TYPE);
        if (contentType == null) {
            return defaultCharset;
        }
        String[] items = contentType.split(";");
        for (String item : items) {
            item = item.trim();
            if (item.isEmpty()) {
                continue;
            }
            int idx = item.indexOf('=');
            if (idx < 0) {
                continue;
            }
            String key = item.substring(0, idx).trim();
            if (key.equalsIgnoreCase("charset")) {
                try {
                    return Charset.forName(item.substring(idx + 1).trim());
                } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
                    return defaultCharset;
                }
            }
        }
        return defaultCharset;
    }

    /**
     * Get charset set in content type header.
     *
     * @return null if no charset is set.
     *
     * @deprecated using {@link #getCharset(Charset)} instead
     */
    @Nullable
    @Deprecated
    public Charset getCharset() {
        return getCharset(null);
    }
}
