package net.dongliu.requests.body;

import net.dongliu.commons.collection.Lists;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Request body parent class
 *
 * @author Liu Dong
 */
public abstract class RequestBody<T> implements Serializable {
    private static final long serialVersionUID = 1332594280620699388L;
    private final T body;
    private String contentType;
    /**
     * If write charset to contentType header value
     */
    private final boolean includeCharset;

    protected RequestBody(T body, String contentType, boolean includeCharset) {
        this.body = body;
        this.contentType = contentType;
        this.includeCharset = includeCharset;
    }

    /**
     * The request body
     *
     * @deprecated use {@link #body()}
     */
    @Deprecated
    public T getBody() {
        return body;
    }

    /**
     * The request body
     */
    public T body() {
        return body;
    }

    /**
     * Set content-type value for this request body
     *
     * @deprecated use {@link RequestBody#contentType(String)}
     */
    @Deprecated
    public RequestBody<T> setContentType(String contentType) {
        this.contentType = requireNonNull(contentType);
        return this;
    }

    /**
     * Set content-type value for this request body
     */
    public RequestBody<T> contentType(String contentType) {
        this.contentType = requireNonNull(contentType);
        return this;
    }

    /**
     * the content type
     *
     * @return may be null if no content type is set
     * @deprecated use {@link #contentType()}
     */
    @Deprecated
    public String getContentType() {
        return contentType;
    }

    /**
     * the content type
     *
     * @return may be null if no content type is set
     */
    public String contentType() {
        return contentType;
    }

    /**
     * If write charset to contentType
     *
     * @deprecated use {@link #includeCharset()}
     */
    @Deprecated
    public boolean isIncludeCharset() {
        return includeCharset;
    }

    /**
     * If write charset to contentType
     */
    public boolean includeCharset() {
        return includeCharset;
    }

    /**
     * Write the request body to output stream.
     *
     * @param out     the output stream to writeTo to
     * @param charset the charset to use
     */
    public void writeTo(OutputStream out, Charset charset) throws IOException {
        writeBody(out, charset);
    }

    /**
     * Write Request body.
     * Note: os should not be closed when this method finished.
     */
    public abstract void writeBody(OutputStream out, Charset charset) throws IOException;

    /**
     * Create request body send json data
     */
    public static <T> RequestBody<T> json(T value) {
        return new JsonRequestBody<>(value);
    }

    /**
     * Create request body send string data
     */
    public static RequestBody<String> text(String value) {
        return new StringRequestBody(requireNonNull(value));
    }

    /**
     * Create request body send x-www-form-encoded data
     */
    public static RequestBody<Collection<? extends Map.Entry<String, ?>>>
    form(Collection<? extends Map.Entry<String, ?>> params) {
        return new FormRequestBody(requireNonNull(params));
    }

    /**
     * Create request body send x-www-form-encoded data
     */
    public static RequestBody<Collection<? extends Map.Entry<String, ?>>> form(Map.Entry<String, ?>... params) {
        return form(Lists.of(params));
    }

    /**
     * Create request body send byte array data
     */
    public static RequestBody<byte[]> bytes(byte[] value) {
        return new BytesRequestBody(requireNonNull(value));
    }

    /**
     * Create request body from input stream.
     *
     * @deprecated Http body may be send multi times(because of redirect or other reasons), use {@link RequestBody#inputStream(InputStreamSupplier)} )} instead.
     */
    @Deprecated
    public static RequestBody<InputStream> inputStream(InputStream in) {
        return new InputStreamRequestBody(requireNonNull(in));
    }

    /**
     * Create request body from input stream.
     */
    public static RequestBody<InputStreamSupplier> inputStream(InputStreamSupplier supplier) {
        return new InputStreamSupplierRequestBody(requireNonNull(supplier));
    }

    /**
     * Create request body from file
     */
    public static RequestBody<File> file(File file) {
        return new FileRequestBody(requireNonNull(file));
    }

    /**
     * Create multi-part post request body
     */
    public static RequestBody<Collection<? extends Part<?>>> multiPart(Collection<? extends Part<?>> parts) {
        return new MultiPartRequestBody(requireNonNull(parts));
    }
}
