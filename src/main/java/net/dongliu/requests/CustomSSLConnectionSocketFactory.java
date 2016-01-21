package net.dongliu.requests;

import net.dongliu.requests.struct.Proxy;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author Dong Liu dongliu@live.cn
 */
public class CustomSSLConnectionSocketFactory extends SSLConnectionSocketFactory {
    private final Proxy proxy;

    public CustomSSLConnectionSocketFactory(SSLContext sslContext, Proxy proxy, boolean verify) {
        super(sslContext, verify ? SSLConnectionSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER
                : SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        this.proxy = proxy;
    }

    @Override
    public Socket createSocket(final HttpContext context) throws IOException {
        if (proxy == null || proxy.getScheme() != Proxy.Scheme.socks) {
            return super.createSocket(context);
        }
        java.net.Proxy proxy = new java.net.Proxy(java.net.Proxy.Type.SOCKS,
                new InetSocketAddress(this.proxy.getHost(), this.proxy.getPort()));
        return new Socket(proxy);
    }

}
