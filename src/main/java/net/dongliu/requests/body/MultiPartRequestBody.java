package net.dongliu.requests.body;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Collection;

/**
 * MultiPart request body
 *
 * @author Liu Dong
 */
class MultiPartRequestBody extends RequestBody<Collection<? extends Part<?>>> {
    private static final String BOUNDARY = "********************" + System.currentTimeMillis();
    private static final String LINE_END = "\r\n";
    private static final long serialVersionUID = -2150328570818986957L;

    public MultiPartRequestBody(Collection<? extends Part<?>> body) {
        super(body, "multipart/form-data; boundary=" + BOUNDARY, false);
    }

    @Override
    public void writeBody(OutputStream out, Charset charset) throws IOException {
        Writer writer = new OutputStreamWriter(out);
        for (Part part : body()) {
            String contentType = part.contentType();
            String name = part.name();
            String fileName = part.fileName();

            writeBoundary(writer);

            writer.write("Content-Disposition: form-data; name=\"" + name + "\"");
            if (fileName != null && !fileName.isEmpty()) {
                writer.write("; filename=\"" + fileName + '"');
            }
            writer.write(LINE_END);

            if (contentType != null && !contentType.isEmpty()) {
                writer.write("Content-Type: " + contentType);
                Charset partCharset = part.charset();
                if (partCharset != null) {
                    writer.write("; charset=" + partCharset.name().toLowerCase());
                }
                writer.write(LINE_END);
            }
            writer.write(LINE_END);
            writer.flush();

            part.writeTo(out);
            out.flush();
            writer.write(LINE_END);
            writer.flush();
            out.flush();
        }
        writer.write("--");
        writer.write(BOUNDARY);
        writer.write("--");
        writer.write(LINE_END);
        writer.flush();
    }

    private void writeBoundary(Writer writer) throws IOException {
        writer.write("--");
        writer.write(BOUNDARY);
        writer.write(LINE_END);
    }

}
