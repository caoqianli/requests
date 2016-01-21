package net.dongliu.requests.struct;

import java.util.Date;

/**
 * http addCookie
 *
 * @author Dong Liu dongliu@live.cn
 */
public class Cookie extends Pair<String, String> {

    private String domain;
    private String path;
    private Date expiry;

    public Cookie(String name, String value) {
        super(name, value);
    }

    public Cookie(String value) {
        super(null, value);
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public Date getExpiry() {
        return expiry;
    }

    public void setExpiry(Date expiry) {
        this.expiry = expiry;
    }
}
