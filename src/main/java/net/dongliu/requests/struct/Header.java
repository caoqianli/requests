package net.dongliu.requests.struct;

/**
 * http header
 *
 * @author Dong Liu dongliu@live.cn
 */
public class Header extends Pair<String, String> {

    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_TYPE_FORM = "application/x-www-form-urlencoded";

    public static final String Accept_Encoding = "Accept-Encoding";
    public static final String Accept_Encoding_COMPRESS = "compress, deflate";

    public Header(String name, Object value) {
        super(name, String.valueOf(value));
    }

}
