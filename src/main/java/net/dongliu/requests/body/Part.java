package net.dongliu.requests.body;

import net.dongliu.commons.Objects2;
import net.dongliu.commons.io.InputStreams;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.*;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Objects.requireNonNull;
import static net.dongliu.requests.HttpHeaders.CONTENT_TYPE_BINARY;


/**
 * This class represent one part(field) of http multipart request body.
 *
 * @author Liu Dong
 */
public class Part<T> implements Serializable {
    private static final long serialVersionUID = -8628605676399143491L;

    /**
     * Filed name
     */
    private final String name;
    /**
     * The file name, for http form's file input.
     * If is text input, this field is null.
     */
    @Nullable
    private final String fileName;
    /**
     * The part content
     */
    private final T body;

    /**
     * The content type of this Part.
     */
    @Nullable
    private final String contentType;

    /**
     * The charset of this part content.
     */
    @Nullable
    private final Charset charset;

    private final PartWriter<T> partWriter;

    private Part(String name, @Nullable String fileName, T body, @Nullable String contentType,
                 @Nullable Charset charset, PartWriter<T> partWriter) {
        this.name = requireNonNull(name);
        this.fileName = fileName;
        this.body = requireNonNull(body);
        this.contentType = contentType;
        this.charset = charset;
        this.partWriter = partWriter;
    }

    /**
     * Set content type for this part.
     */
    public Part<T> contentType(String contentType) {
        requireNonNull(contentType);
        return new Part<>(name, fileName, body, contentType, charset, partWriter);
    }

    /**
     * The charset of this part's content. Each part of MultiPart body can has it's own charset set.
     * Default not set.
     *
     * @param charset the charset
     * @return self
     */
    public Part<T> charset(Charset charset) {
        requireNonNull(charset);
        return new Part<>(name, fileName, body, contentType, charset, partWriter);
    }

    /**
     * Create a file multi-part field, from file.
     * This return a part equivalent to &lt;input type="file" /&gt; field in multi part form.
     */
    public static Part<File> file(String name, File file) {
        return file(name, file.getName(), file);
    }

    /**
     * Create a file multi-part field, from file.
     * This return a part equivalent to &lt;input type="file" /&gt; field in multi part form.
     */
    public static Part<File> file(String name, String fileName, File file) {
        return new Part<>(name, fileName, file, ContentTypes.probeContentType(file), null, (body, out, charset) -> {
            try (InputStream in = new FileInputStream(body)) {
                InputStreams.transferTo(in, out);
            }
        });
    }


    /**
     * Create a file multi-part field, from InputStream.
     * This return a part equivalent to &lt;input type="file" /&gt; field in multi part form.
     *
     * @deprecated Http body may be send multi times(because of redirect or other reasons), use {@link Part#file(String, String, InputStreamSupplier)} instead.
     */
    @Deprecated
    public static Part<InputStream> file(String name, String fileName, InputStream in) {
        return new Part<>(name, fileName, in, CONTENT_TYPE_BINARY, null, (body, out, charset) -> {
            try (InputStream bin = body) {
                InputStreams.transferTo(bin, out);
            }
        });
    }

    /**
     * Create a file multi-part field, from InputStream.
     * This return a part equivalent to &lt;input type="file" /&gt; field in multi part form.
     */
    public static Part<InputStreamSupplier> file(String name, String fileName, InputStreamSupplier supplier) {
        return new Part<>(name, fileName, supplier, CONTENT_TYPE_BINARY, null, (body, out, charset) -> {
            try (InputStream bin = body.get()) {
                InputStreams.transferTo(bin, out);
            }
        });
    }

    /**
     * Create a file multi-part field, from byte array data.
     * This return a part equivalent to &lt;input type="file" /&gt; field in multi part form.
     */
    public static Part<byte[]> file(String name, String fileName, byte[] bytes) {
        return new Part<>(name, fileName, bytes, CONTENT_TYPE_BINARY, null, (body, out, charset) -> out.write(body));
    }

    /**
     * Create a text multi-part field.
     * This return a part equivalent to &lt;input type="text" /&gt; field in multi part form.
     */
    public static Part<String> text(String name, String value) {
        // the text part do not set content type
        return new Part<>(name, null, value, null, null, (body, out, charset) -> {
            OutputStreamWriter writer = new OutputStreamWriter(out, Objects2.elvis(charset, ISO_8859_1));
            writer.write(body);
            writer.flush();
        });
    }

    /**
     * Create a (name, value) text multi-part field.
     * This return a part equivalent to &lt;input type="text" /&gt; field in multi part form.
     *
     * @deprecated use {@link #text(String, String)} instead.
     */
    @Deprecated
    public static Part<String> param(String name, String value) {
        return text(name, value);
    }

    /**
     * @deprecated use {@link #name()}
     */
    @Deprecated
    public String getName() {
        return name;
    }

    /**
     * @deprecated use {@link #fileName()}
     */
    @Deprecated
    @Nullable
    public String getFileName() {
        return fileName;
    }

    /**
     * @deprecated use {@link #body()}
     */
    @Deprecated
    public T getBody() {
        return body;
    }

    /**
     * @deprecated use {@link #contentType()}
     */
    @Deprecated
    @Nullable
    public String getContentType() {
        return contentType;
    }

    /**
     * @deprecated use {@link #charset()}
     */
    @Deprecated
    @Nullable
    public Charset getCharset() {
        return charset;
    }

    /**
     * The part field name
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * The filename of th part. may be null if not exists
     */
    @Nullable
    public String fileName() {
        return fileName;
    }

    /**
     * The part body
     */
    public T body() {
        return body;
    }

    /**
     * The content type of the part. For text part, the contentType is always null.
     */
    @Nullable
    public String contentType() {
        return contentType;
    }


    /**
     * The charset of this part's content. Each part of MultiPart body can has it's own charset set.
     * For text part, the charset is always null.
     *
     * @return the charset of this part content. if not set, return null.
     */
    @Nullable
    public Charset charset() {
        return charset;
    }

    /**
     * Write part content to output stream.
     */
    public void writeTo(OutputStream out) throws IOException {
        partWriter.writeTo(body, out, charset);
    }


    private interface PartWriter<T> {
        void writeTo(T body, OutputStream out, @Nullable Charset charset) throws IOException;
    }
}
