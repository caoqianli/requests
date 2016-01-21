package net.dongliu.requests;

import net.dongliu.requests.struct.Proxy;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author Dong Liu dongliu@live.cn
 */
public class CustomConnectionSocketFactory extends PlainConnectionSocketFactory {
    private final Proxy proxy;

    public CustomConnectionSocketFactory(Proxy proxy) {
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
