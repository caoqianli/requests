package net.dongliu.requests.utils;

import net.dongliu.requests.Cookie;
import net.dongliu.requests.Parameter;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Date;

/**
 * Only for internal use.
 * Http response set cookie header.
 * <p>
 * RFC 6265:
 * The origin domain of a cookie is the domain of the originating request.
 * If the origin domain is an IP, the cookie's domain attribute must not be set.
 * If a cookie's domain attribute is not set, the cookie is only applicable to its origin domain.
 * If a cookie's domain attribute is set,
 * -- the cookie is applicable to that domain and all its subdomains;
 * -- the cookie's domain must be the same as, or a parent of, the origin domain
 * -- the cookie's domain must not be a TLD, a public suffix, or a parent of a public suffix.
 */
public class Cookies {

    /**
     * Get cookie default path from url path
     */
    public static String calculatePath(String uri) {
        if (!uri.startsWith("/")) {
            return "/";
        }
        int idx = uri.lastIndexOf('/');
        return uri.substring(0, idx + 1);
    }

    /**
     * A simple, non-accurate method(we just need to distinguish ip and domain) to judge if host is a ipv4/ipv6 address.
     */
    public static boolean isIP(String host) {
        return isIPV4(host) || isIPV6(host);
    }


    private static boolean isIPV4(String host) {
        int dotCount = 0;
        for (int i = 0; i < host.length(); i++) {
            char c = host.charAt(i);
            if (c >= '0' && c <= '9') {
                //
            } else if (c == '.') {
                if (++dotCount > 3) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    private static boolean isIPV6(String host) {
        int colonCount = 0;
        int dotCount = 0;
        for (int i = 0; i < host.length(); i++) {
            char c = host.charAt(i);
            if (c >= '0' && c <= '9') {
                //
            } else if (c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
                if (dotCount > 0) {
                    return false;
                }
            } else if (c == ':') {
                colonCount++;
                if (colonCount + dotCount > 7) {
                    return false;
                }
            } else if (c == '.') {
                dotCount++;
                if (colonCount < 2 || dotCount > 3) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }


    /**
     * If domainSuffix is suffix of domain
     *
     * @param domain       start with "."
     * @param domainSuffix not start with "."
     */
    public static boolean isDomainSuffix(String domain, String domainSuffix) {
        if (domain.length() < domainSuffix.length()) {
            return false;
        }
        if (domain.length() == domainSuffix.length()) {
            return domain.equals(domainSuffix);
        }

        return domain.endsWith(domainSuffix) && domain.charAt(domain.length() - domainSuffix.length() - 1) == '.';
    }


    /**
     * If cookie match the given scheme, host, and path.
     *
     * @param host should be lower case
     */
    public static boolean match(Cookie cookie, String protocol, String host, String path) {
        if (cookie.secure() && !protocol.equalsIgnoreCase("https")) {
            return false;
        }

        // check domain
        if (isIP(host) || cookie.hostOnly()) {
            if (!host.equals(cookie.domain())) {
                return false;
            }
        } else {
            if (!Cookies.isDomainSuffix(host, cookie.domain())) {
                return false;
            }
        }

        String cookiePath = cookie.path();
        // check path
        if (cookiePath.length() > path.length()) {
            return false;
        }
        if (cookiePath.length() == path.length()) {
            return cookiePath.equals(path);
        }
        if (!path.startsWith(cookiePath)) {
            return false;
        }
        if (cookiePath.charAt(cookiePath.length() - 1) != '/' && path.charAt(cookiePath.length()) != '/') {
            return false;
        }
        return true;
    }

    /**
     * Parse one cookie header value, return the cookie.
     *
     * @param host should be lower case
     * @return return null means is not a valid cookie str.
     */
    @Nullable
    public static Cookie parseCookie(String cookieStr, String host, String defaultPath) {
        String[] items = cookieStr.split(";");
        Parameter<String> param = parseCookieNameValue(items[0]);
        if (param == null) {
            return null;
        }

        String domain = "";
        String path = "";
        long expiry = 0;
        boolean secure = false;
        for (String item : items) {
            item = item.trim();
            if (item.isEmpty()) {
                continue;
            }
            Parameter<String> attribute = parseCookieAttribute(item);
            switch (attribute.name().toLowerCase()) {
                case "domain":
                    domain = normalizeDomain(attribute.value());
                    break;
                case "path":
                    path = normalizePath(attribute.value());
                    break;
                case "expires":
                    // If a cookie has both the Max-Age and the Expires attribute, the Max-Age attribute has precedence
                    // and controls the expiration date of the cookie.
                    if (expiry == 0) {
                        Date date = CookieDateUtil.parseDate(attribute.value());
                        if (date != null) {
                            expiry = date.getTime();
                            if (expiry == 0) {
                                expiry = 1;
                            }
                        }
                    }
                    break;
                case "max-age":
                    try {
                        int seconds = Integer.parseInt(attribute.value());
                        expiry = System.currentTimeMillis() + seconds * 1000;
                        if (expiry == 0) {
                            expiry = 1;
                        }
                    } catch (NumberFormatException ignore) {
                    }
                    break;
                case "secure":
                    secure = true;
                    break;
                case "httponly":
                    // ignore http only now
                    break;
                default:
            }
        }

        if (path.isEmpty()) {
            path = defaultPath;
        }
        boolean hostOnly;
        if (domain.isEmpty()) {
            domain = host;
            hostOnly = true;
        } else {
            if (isIP(host)) {
                // should not set
                return null;
            }
            if (!isDomainSuffix(host, domain)) {
                return null;
            }
            hostOnly = false;
        }

        return new Cookie(domain, path, param.name(), param.value(), expiry, secure, hostOnly);
    }


    @Nullable
    private static Parameter<String> parseCookieNameValue(String str) {
        // Browsers always split the name and value on the first = symbol in the string
        int idx = str.indexOf("=");
        if (idx <= 0) {
            // If there is no = symbol in the string at all, RFC 6265 ignore this cookie.
            // if cookie name is empty, RFC 6265 ignore this cookie.
            return null;
        }
        return Parameter.of(str.substring(0, idx).trim(), str.substring(idx + 1).trim());
    }

    private static Parameter<String> parseCookieAttribute(String str) {
        int idx = str.indexOf("=");
        if (idx < 0) {
            return Parameter.of(str, "");
        } else {
            return Parameter.of(str.substring(0, idx), str.substring(idx + 1));
        }
    }

    private static String normalizePath(String str) {
        if (!str.startsWith("/")) {
            // use defaultPath
            return "";
        }
        return str;
    }

    /**
     * Parse cookie domain.
     * In RFC 6265, the leading dot will be ignored, and cookie always available in sub domains.
     *
     * @return the final domain
     */
    private static String normalizeDomain(String value) {
        if (value.startsWith(".")) {
            return value.substring(1);
        }
        return value.toLowerCase();
    }
}
