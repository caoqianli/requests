package net.dongliu.requests;

import net.dongliu.commons.Lazy;
import net.dongliu.commons.io.Closeables;
import net.dongliu.commons.io.InputStreams;
import net.dongliu.commons.io.Readers;
import net.dongliu.requests.ResponseHandler.ResponseInfo;
import net.dongliu.requests.exception.RequestsException;
import net.dongliu.requests.json.JsonLookup;
import net.dongliu.requests.json.TypeInfer;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static net.dongliu.requests.HttpHeaders.NAME_CONTENT_ENCODING;
import static net.dongliu.requests.StatusCodes.NOT_MODIFIED;
import static net.dongliu.requests.StatusCodes.NO_CONTENT;

/**
 * Raw http response.
 * It you do not consume http response body, with readToText, readToBytes, writeToFile, toTextResponse,
 * toJsonResponse, etc.., you need to close this raw response manually
 *
 * @author Liu Dong
 */
public class RawResponse extends AbstractResponse implements AutoCloseable {
    private final String method;
    private final String statusLine;
    private final InputStream body;
    @Nullable
    private final Charset charset;
    private final boolean decompress;

    public RawResponse(String method, String url, int statusCode, String statusLine, List<Cookie> cookies,
                       Headers headers, InputStream body, Charset charset, boolean decompress) {
        super(url, statusCode, cookies, headers);
        this.method = method;
        this.statusLine = statusLine;
        this.body = body;
        this.charset = charset;
        this.decompress = decompress;
    }

    // Only for internal use. Do not call this method.
    public RawResponse(String method, String url, int statusCode, String statusLine, List<Cookie> cookies, Headers headers,
                       InputStream input, HttpURLConnection conn) {
        super(url, statusCode, cookies, headers);
        this.method = method;
        this.statusLine = statusLine;
        this.body = new HttpConnInputStream(input, conn);
        this.charset = null;
        this.decompress = true;
    }


    @Override
    public void close() {
        try {
            body.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Return a new RawResponse instance with response body charset set.
     * If charset is not set(which is default), will try to get charset from response headers; If failed, use UTF-8.
     *
     * @deprecated use {{@link #charset(Charset)}} instead
     */
    @Deprecated
    public RawResponse withCharset(Charset charset) {
        return charset(charset);
    }

    /**
     * Set response read charset.
     * If not set, would get charset from response headers. If not found, would use UTF-8.
     */
    public RawResponse charset(Charset charset) {
        return new RawResponse(method, url, statusCode, statusLine, cookies, headers, body, charset, decompress);
    }

    /**
     * Set response read charset.
     * If not set, would get charset from response headers. If not found, would use UTF-8.
     */
    public RawResponse charset(String charset) {
        return charset(Charset.forName(requireNonNull(charset)));
    }

    /**
     * If decompress http response body. Default is true.
     */
    public RawResponse decompress(boolean decompress) {
        return new RawResponse(method, url, statusCode, statusLine, cookies, headers, body, charset, decompress);
    }

    /**
     * Read response body to string. return empty string if response has no body
     */
    public String readToText() {
        Charset charset = getCharset();
        try (InputStream in = body();
             Reader reader = new InputStreamReader(in, charset)) {
            return Readers.readAll(reader);
        } catch (IOException e) {
            throw new RequestsException(e);
        } finally {
            close();
        }
    }

    /**
     * Convert to response, with body as text. The origin raw response will be closed
     */
    public Response<String> toTextResponse() {
        return new Response<>(this.url, this.statusCode, this.cookies, this.headers, readToText());
    }

    /**
     * Read response body to byte array. return empty byte array if response has no body
     */
    public byte[] readToBytes() {
        try {
            try (InputStream in = body()) {
                return InputStreams.readAll(in);
            }
        } catch (IOException e) {
            throw new RequestsException(e);
        } finally {
            close();
        }
    }

    /**
     * Handle response body with handler, return a new response with content as handler result.
     * The response is closed whether this call succeed or failed with exception.
     */
    public <T> Response<T> toResponse(ResponseHandler<T> handler) {
        ResponseInfo responseInfo = new ResponseInfo(this.url, this.statusCode, this.headers, body());
        try {
            T result = handler.handle(responseInfo);
            return new Response<>(this.url, this.statusCode, this.cookies, this.headers, result);
        } catch (IOException e) {
            throw new RequestsException(e);
        } finally {
            close();
        }
    }

    /**
     * Convert to response, with body as byte array
     */
    public Response<byte[]> toBytesResponse() {
        return new Response<>(this.url, this.statusCode, this.cookies, this.headers, readToBytes());
    }

    /**
     * Deserialize response content as json
     *
     * @return null if json value is null or empty
     */
    public <T> T readToJson(Type type) {
        try {
            return JsonLookup.getInstance().lookup().unmarshal(body(), getCharset(), type);
        } catch (IOException e) {
            throw new RequestsException(e);
        } finally {
            close();
        }
    }

    /**
     * Deserialize response content as json
     *
     * @return null if json value is null or empty
     */
    public <T> T readToJson(TypeInfer<T> typeInfer) {
        return readToJson(typeInfer.getType());
    }

    /**
     * Deserialize response content as json
     *
     * @return null if json value is null or empty
     */
    public <T> T readToJson(Class<T> cls) {
        return readToJson((Type) cls);
    }

    /**
     * Convert http response body to json result
     */
    public <T> Response<T> toJsonResponse(TypeInfer<T> typeInfer) {
        return new Response<>(this.url, this.statusCode, this.cookies, this.headers, readToJson(typeInfer));
    }

    /**
     * Convert http response body to json result
     */
    public <T> Response<T> toJsonResponse(Class<T> cls) {
        return new Response<>(this.url, this.statusCode, this.cookies, this.headers, readToJson(cls));
    }

    /**
     * Write response body to file
     */
    public void writeToFile(File file) {
        try {
            try (OutputStream os = new FileOutputStream(file)) {
                InputStreams.transferTo(body(), os);
            }
        } catch (IOException e) {
            throw new RequestsException(e);
        } finally {
            close();
        }
    }

    /**
     * Write response body to file
     */
    public void writeToFile(Path path) {
        try {
            try (OutputStream os = Files.newOutputStream(path)) {
                InputStreams.transferTo(body(), os);
            }
        } catch (IOException e) {
            throw new RequestsException(e);
        } finally {
            close();
        }
    }


    /**
     * Write response body to file
     */
    public void writeToFile(String path) {
        try {
            try (OutputStream os = new FileOutputStream(path)) {
                InputStreams.transferTo(body(), os);
            }
        } catch (IOException e) {
            throw new RequestsException(e);
        } finally {
            close();
        }
    }

    /**
     * Write response body to file, and return response contains the file.
     */
    public Response<File> toFileResponse(Path path) {
        File file = path.toFile();
        this.writeToFile(file);
        return new Response<>(this.url, this.statusCode, this.cookies, this.headers, file);
    }

    /**
     * Write response body to OutputStream. OutputStream will not be closed.
     */
    public void writeTo(OutputStream out) {
        try {
            InputStreams.transferTo(body(), out);
        } catch (IOException e) {
            throw new RequestsException(e);
        } finally {
            close();
        }
    }

    /**
     * Write response body to Writer, charset can be set using {@link #charset(Charset)},
     * or will use charset detected from response header if not set.
     * Writer will not be closed.
     */
    public void writeTo(Writer writer) {
        try {
            try (InputStream in = body();
                 Reader reader = new InputStreamReader(in, getCharset())) {
                Readers.transferTo(reader, writer);
            }
        } catch (IOException e) {
            throw new RequestsException(e);
        } finally {
            close();
        }
    }

    /**
     * Consume and discard this response body.
     */
    public void discardBody() {
        try (InputStream in = body) {
            InputStreams.discardAll(in);
        } catch (IOException e) {
            throw new RequestsException(e);
        } finally {
            close();
        }
    }

    /**
     * Get the status line
     *
     * @deprecated use {@link #statusLine()}
     */
    @Deprecated
    public String getStatusLine() {
        return statusLine;
    }

    /**
     * Get the status line
     */
    public String statusLine() {
        return statusLine;
    }

    private Lazy<InputStream> decompressedBody = Lazy.of(this::decompressBody);

    /**
     * The response body input stream
     *
     * @deprecated use {@link #body()}
     */
    @Deprecated
    public InputStream getInput() {
        return decompressedBody.get();
    }

    public String method() {
        return method;
    }

    /**
     * The response body input stream
     */
    public InputStream body() {
        return decompressedBody.get();
    }

    @Nullable
    public Charset charset() {
        return charset;
    }

    public boolean decompress() {
        return decompress;
    }

    private Charset getCharset() {
        if (this.charset != null) {
            return this.charset;
        }
        return headers.getCharset(UTF_8);
    }


    /**
     * Wrap response input stream if it is compressed, return input its self if not use compress
     */
    private InputStream decompressBody() {
        if (!decompress) {
            return body;
        }
        // if has no body, some server still set content-encoding header,
        // GZIPInputStream wrap empty input stream will cause exception. we should check this
        if (method.equals(Methods.HEAD)
                || (statusCode >= 100 && statusCode < 200) || statusCode == NOT_MODIFIED || statusCode == NO_CONTENT) {
            return body;
        }

        String contentEncoding = headers.getHeader(NAME_CONTENT_ENCODING);
        if (contentEncoding == null) {
            return body;
        }

        //we should remove the content-encoding header here?
        switch (contentEncoding) {
            case "gzip":
                try {
                    return new GZIPInputStream(body);
                } catch (IOException e) {
                    Closeables.closeQuietly(body);
                    throw new RequestsException(e);
                }
            case "deflate":
                // Note: deflate implements may or may not wrap in zlib due to rfc confusing.
                // here deal with deflate without zlib header
                return new InflaterInputStream(body, new Inflater(true));
            case "identity":
            case "compress": //historic; deprecated in most applications and replaced by gzip or deflate
            default:
                return body;
        }
    }
}
