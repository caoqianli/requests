package net.dongliu.requests;

import net.dongliu.requests.body.RequestBody;
import net.dongliu.requests.executor.SessionContext;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serializable;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Http request
 *
 * @author Liu Dong
 */
public class Request implements Serializable {
    private static final long serialVersionUID = -2585065451136206831L;
    private final String method;
    private final Collection<? extends Map.Entry<String, ?>> headers;
    private final Collection<? extends Map.Entry<String, ?>> cookies;
    private final Collection<? extends Map.Entry<String, ?>> params;

    private final String userAgent;
    private final Charset charset;
    @Nullable
    private final RequestBody<?> body;
    private final int socksTimeout;
    private final int connectTimeout;
    @Nullable
    private final Proxy proxy;
    private final boolean followRedirect;
    private final int maxRedirectCount;
    private final boolean acceptCompress;
    private final boolean verify;
    @Nullable
    private final KeyStore keyStore;
    @Nullable
    private final BasicAuth basicAuth;
    @Nullable
    private final SessionContext sessionContext;
    private final URL url;
    private final boolean keepAlive;

    Request(RequestBuilder builder) {
        method = builder.method;
        headers = builder.headers;
        cookies = builder.cookies;
        userAgent = builder.userAgent;
        charset = builder.charset;
        body = builder.body;
        socksTimeout = builder.socksTimeout;
        connectTimeout = builder.connectTimeout;
        proxy = builder.proxy;
        followRedirect = builder.followRedirect;
        maxRedirectCount = builder.maxRedirectCount;
        acceptCompress = builder.acceptCompress;
        verify = builder.verify;
        keyStore = builder.keyStore;
        basicAuth = builder.basicAuth;
        sessionContext = builder.sessionContext;
        keepAlive = builder.keepAlive;
        this.url = builder.url;
        this.params = builder.params;
    }

    /**
     * Create and copy fields to mutable builder instance.
     */
    public RequestBuilder toBuilder() {
        return new RequestBuilder(this);
    }

    /**
     * @deprecated use {@link #method()}
     */
    @Deprecated
    public String getMethod() {
        return method;
    }

    /**
     * @deprecated use {@link #headers()}
     */
    @Deprecated
    public Collection<? extends Map.Entry<String, ?>> getHeaders() {
        return headers;
    }

    /**
     * @deprecated use {@link #cookies()}
     */
    @Deprecated
    public Collection<? extends Map.Entry<String, ?>> getCookies() {
        return cookies;
    }

    /**
     * @deprecated use {@link #userAgent()}
     */
    @Deprecated
    public String getUserAgent() {
        return userAgent;
    }

    /**
     * @deprecated use {@link #charset()}
     */
    @Deprecated
    public Charset getCharset() {
        return charset;
    }

    /**
     * @deprecated use {@link #body()}
     */
    @Deprecated
    @Nullable
    public RequestBody<?> getBody() {
        return body;
    }

    /**
     * @deprecated use {@link #socksTimeout()}
     */
    @Deprecated
    public int getSocksTimeout() {
        return socksTimeout;
    }

    /**
     * @deprecated use {@link #connectTimeout()}
     */
    @Deprecated
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * @deprecated use {@link #proxy()}
     */
    @Deprecated
    @Nullable
    public Proxy getProxy() {
        return proxy;
    }

    /**
     * @deprecated use {@link #followRedirect()}
     */
    @Deprecated
    public boolean isFollowRedirect() {
        return followRedirect;
    }

    /**
     * @deprecated use {@link #maxRedirectCount()}
     */
    @Deprecated
    public int getMaxRedirectCount() {
        return maxRedirectCount;
    }

    /**
     * @deprecated use {@link #acceptCompress()}
     */
    @Deprecated
    public boolean isCompress() {
        return acceptCompress;
    }

    /**
     * @deprecated use {@link #verify()}
     */
    @Deprecated
    public boolean isVerify() {
        return verify;
    }

    /**
     * @deprecated use {@link #keyStore()}
     */
    @Deprecated
    @Nullable
    public KeyStore getKeyStore() {
        return keyStore;
    }

    /**
     * @deprecated use {@link #basicAuth()}
     */
    @Deprecated
    public BasicAuth getBasicAuth() {
        return basicAuth;
    }

    /**
     * @deprecated use {@link #sessionContext()}
     */
    @Deprecated
    @Nullable
    public SessionContext getSessionContext() {
        return sessionContext;
    }

    /**
     * @deprecated use {@link #url()}
     */
    @Deprecated
    public URL getUrl() {
        return url;
    }


    /**
     * Parameter to append to url.
     * @deprecated use {@link #params()}
     */
    @Deprecated
    public Collection<? extends Map.Entry<String, ?>> getParams() {
        return params;
    }

    /**
     * @deprecated use {@link #keepAlive()}
     */
    @Deprecated
    public boolean isKeepAlive() {
        return keepAlive;
    }


    public String method() {
        return method;
    }

    public Collection<? extends Entry<String, ?>> headers() {
        return headers;
    }

    public Collection<? extends Entry<String, ?>> cookies() {
        return cookies;
    }

    public Collection<? extends Entry<String, ?>> params() {
        return params;
    }

    public String userAgent() {
        return userAgent;
    }

    public Charset charset() {
        return charset;
    }

    @Nullable
    public RequestBody<?> body() {
        return body;
    }

    public int socksTimeout() {
        return socksTimeout;
    }

    public int connectTimeout() {
        return connectTimeout;
    }

    @Nullable
    public Proxy proxy() {
        return proxy;
    }

    public boolean followRedirect() {
        return followRedirect;
    }

    public int maxRedirectCount() {
        return maxRedirectCount;
    }

    public boolean acceptCompress() {
        return acceptCompress;
    }

    public boolean verify() {
        return verify;
    }

    public KeyStore keyStore() {
        return keyStore;
    }

    public BasicAuth basicAuth() {
        return basicAuth;
    }

    public SessionContext sessionContext() {
        return sessionContext;
    }

    public URL url() {
        return url;
    }

    public boolean keepAlive() {
        return keepAlive;
    }
}
