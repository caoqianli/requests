package net.dongliu.requests;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * Common parent for RawResponse and Response
 */
class AbstractResponse {
    protected final String url;
    protected final int statusCode;
    protected final List<Cookie> cookies;
    protected final Headers headers;

    protected AbstractResponse(String url, int statusCode, List<Cookie> cookies, Headers headers) {
        this.url = url;
        this.statusCode = statusCode;
        this.cookies = Collections.unmodifiableList(cookies);
        this.headers = headers;
    }

    /**
     * Get actual url (redirected)
     * @deprecated using {@link #url}
     */
    @Deprecated
    public String getURL() {
        return url;
    }

    /**
     * return actual url (redirected)
     */
    public String url() {
        return url;
    }

    /**
     * return response status code
     * @return status code
     * @deprecated using {@link #statusCode}
     */
    @Deprecated
    public int getStatusCode() {
        return statusCode;
    }

    /**
     * return response status code
     * @return status code
     */
    public int statusCode() {
        return statusCode;
    }

    /**
     * Get all cookies returned by this response
     * @deprecated using {@link #cookies}
     */
    @Deprecated
    public List<Cookie> getCookies() {
        return cookies;
    }

    /**
     * Get all cookies returned by this response
     */

    public List<Cookie> cookies() {
        return cookies;
    }

    /**
     * Get all response headers
     * @deprecated using {@link #headers}
     */
    @Deprecated
    public List<Header> getHeaders() {
        return headers.getHeaders();
    }

    /**
     * Return all response headers
     */
    public List<Header> headers() {
        return headers.getHeaders();
    }

    /**
     * Get first cookie match the name returned by this response, return null if not found
     *
     * @deprecated using {{@link #getCookie(String)}} instead
     */
    @Deprecated
    @Nullable
    public Cookie getFirstCookie(String name) {
        return getCookie(name);
    }

    /**
     * Get first cookie match the name returned by this response, return null if not found
     */
    @Nullable
    public Cookie getCookie(String name) {
        for (Cookie cookie : cookies) {
            if (cookie.name().equals(name)) {
                return cookie;
            }
        }
        return null;
    }

    /**
     * Get first header value match the name, return null if not exists
     *
     * @deprecated using {@link #getHeader(String)} instead
     */
    @Deprecated
    @Nullable
    public String getFirstHeader(String name) {
        return headers.getFirstHeader(name);
    }

    /**
     * Get first header value match the name, return null if not exists
     */
    @Nullable
    public String getHeader(String name) {
        return headers.getHeader(name);
    }

    /**
     * Get all headers values with name. If not exists, return empty list
     */
    public List<String> getHeaders(String name) {
        return this.headers.getHeaders(name);
    }

}
