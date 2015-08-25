package net.dongliu.requests;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import net.dongliu.requests.async.AsyncIdleConnectionMonitorThread;
import net.dongliu.requests.exception.RequestException;
import net.dongliu.requests.struct.Host;
import net.dongliu.requests.struct.Pair;
import net.dongliu.requests.struct.Proxy;
import org.apache.http.HttpHost;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.config.Registry;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.client.util.HttpAsyncClientUtils;
import org.apache.http.nio.reactor.IOReactorException;

/**
 * Pooled http client use connection pool, for reusing http connection across http requests.
 * This class is thread-safe, can service connection requests from multiple execution threads.
 *
 * @author Dong Liu dongliu@wandoujia.com
 */
public class PooledClient implements Closeable {
	
	// the wrapped http client
	private CloseableHttpClient client;
	private CloseableHttpAsyncClient asyncClient;
	private AsyncIdleConnectionMonitorThread asyncIdleConnectionMonitorThread;
	
	private final PooledClientBuilder pooledClientBuilder;
	
	public PooledClient(PooledClientBuilder pooledClientBuilder) {
		this.pooledClientBuilder = pooledClientBuilder;
	}
	
	/**
	 * create new ConnectionPool Builder.
	 */
	public static PooledClientBuilder custom() {
		return new PooledClientBuilder();
	}
	
	@Override
	public void close() {
		HttpClientUtils.closeQuietly(client);
		HttpAsyncClientUtils.closeQuietly(asyncClient);
		asyncIdleConnectionMonitorThread.interrupt();
	}
	
	Proxy getProxy() {
		return pooledClientBuilder.proxy;
	}
	
	/**
	 * get method
	 */
	public RequestBuilder get(String url) throws RequestException {
		return Requests.get(url).connectionPool(this);
	}
	
	/**
	 * head method
	 */
	public RequestBuilder head(String url) throws RequestException {
		return Requests.head(url).connectionPool(this);
	}
	
	/**
	 * get url, and return content
	 */
	public RequestBuilder post(String url) throws RequestException {
		return Requests.post(url).connectionPool(this);
	}
	
	/**
	 * put method
	 */
	public RequestBuilder put(String url) throws RequestException {
		return Requests.put(url).connectionPool(this);
	}
	
	/**
	 * delete method
	 */
	public RequestBuilder delete(String url) throws RequestException {
		return Requests.delete(url).connectionPool(this);
	}
	
	/**
	 * options method
	 */
	public RequestBuilder options(String url) throws RequestException {
		return Requests.options(url).connectionPool(this);
	}
	
	/**
	 * patch method
	 */
	public RequestBuilder patch(String url) throws RequestException {
		return Requests.patch(url).connectionPool(this);
	}
	
	/**
	 * trace method
	 */
	public RequestBuilder trace(String url) throws RequestException {
		return Requests.trace(url).connectionPool(this);
	}
	
	/**
	 * create a session. session can do request as Requests do, and keep cookies to maintain a http session
	 */
	public Session session() {
		return new Session(this);
	}
	
	CloseableHttpClient getHttpClient() {
		if (client == null) {
			buildHttpClient();
		}
		return client;
	}
	
	CloseableHttpAsyncClient getHttpAsyncClient() throws IOReactorException {
		if (asyncClient == null) {
			buildHttpAsyncClient();
			asyncClient.start();
		}
		return asyncClient;
	}
	
	void buildHttpClient() {
		Registry<ConnectionSocketFactory> r = Utils.getConnectionSocketFactoryRegistry(pooledClientBuilder.proxy, pooledClientBuilder.verify);
		PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager(r,
		                                                                                    null,
		                                                                                    null,
		                                                                                    null,
		                                                                                    pooledClientBuilder.timeToLive,
		                                                                                    TimeUnit.MILLISECONDS);
		
		manager.setMaxTotal(pooledClientBuilder.maxTotal);
		manager.setDefaultMaxPerRoute(pooledClientBuilder.maxPerRoute);
		if (pooledClientBuilder.perRouteCount != null) {
			for (Pair<Host, Integer> pair : pooledClientBuilder.perRouteCount) {
				Host host = pair.getName();
				manager.setMaxPerRoute(new HttpRoute(new HttpHost(host.getDomain(), host.getPort())), pair.getValue());
			}
		}
		
		HttpClientBuilder clientBuilder = HttpClients.custom().setUserAgent(pooledClientBuilder.userAgent).setConnectionManager(manager);
		
		// disable gzip
		if (!pooledClientBuilder.gzip) {
			clientBuilder.disableContentCompression();
		}
		
		if (!pooledClientBuilder.allowRedirects) {
			clientBuilder.disableRedirectHandling();
		}
		
		if (pooledClientBuilder.allowPostRedirects) {
			clientBuilder.setRedirectStrategy(new AllRedirectStrategy());
		}
		client = clientBuilder.build();
	}
	
	void buildHttpAsyncClient() throws IOReactorException {
		PoolingNHttpClientConnectionManager asyncConnectionManager = new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor(),
		                                                                                                     null,
		                                                                                                     Utils.getSchemeIOSessionStrategy(pooledClientBuilder.verify),
		                                                                                                     null,
		                                                                                                     null,
		                                                                                                     pooledClientBuilder.timeToLive,
		                                                                                                     TimeUnit.MILLISECONDS);
		asyncConnectionManager.setMaxTotal(pooledClientBuilder.maxTotal);
		asyncConnectionManager.setDefaultMaxPerRoute(pooledClientBuilder.maxPerRoute);
		if (pooledClientBuilder.perRouteCount != null) {
			for (Pair<Host, Integer> pair : pooledClientBuilder.perRouteCount) {
				Host host = pair.getName();
				asyncConnectionManager.setMaxPerRoute(new HttpRoute(new HttpHost(host.getDomain(), host.getPort())), pair.getValue());
			}
		}
		HttpAsyncClientBuilder asyncClientBuilder = HttpAsyncClientBuilder.create()
		                                                                  .setUserAgent(pooledClientBuilder.userAgent)
		                                                                  .setConnectionManager(asyncConnectionManager);
		
		if (pooledClientBuilder.allowPostRedirects) {
			asyncClientBuilder.setRedirectStrategy(new AllRedirectStrategy());
		}
		
		asyncIdleConnectionMonitorThread = new AsyncIdleConnectionMonitorThread(asyncConnectionManager);
		asyncIdleConnectionMonitorThread.start();
		
		asyncClient = asyncClientBuilder.build();
	}
	
	public static class PooledClientBuilder {
		// how long http connection keep, in milliseconds. default -1, get from server response
		private long timeToLive = -1;
		// the max total http connection count
		private int maxTotal = 20;
		// the max connection count for each host
		private int maxPerRoute = 2;
		// set max count for specified host
		private List<Pair<Host, Integer>> perRouteCount;
		
		private Proxy proxy;
		
		// settings for client level, can not set/override in request level
		// if verify http certificate
		private boolean verify = true;
		// if enable gzip response
		private boolean gzip = true;
		private boolean allowRedirects = true;
		private boolean allowPostRedirects = false;
		private String userAgent = Utils.defaultUserAgent;
		
		private AsyncIdleConnectionMonitorThread asyncIdleConnectionMonitorThread;
		
		PooledClientBuilder() {
		}
		
		public PooledClient build() {
			return new PooledClient(this);
		}
		
		/**
		 * how long http connection keep, in milliseconds. default -1, get from server response
		 */
		public PooledClientBuilder timeToLive(long timeToLive) {
			this.timeToLive = timeToLive;
			return this;
		}
		
		/**
		 * the max total http connection count. default 20
		 */
		public PooledClientBuilder maxTotal(int maxTotal) {
			this.maxTotal = maxTotal;
			return this;
		}
		
		/**
		 * set default max connection count for each host, default 2
		 */
		public PooledClientBuilder maxPerRoute(int maxPerRoute) {
			this.maxPerRoute = maxPerRoute;
			return this;
		}
		
		/**
		 * set specified max connection count for the host, default 2
		 */
		public PooledClientBuilder maxPerRoute(Host host, int maxPerRoute) {
			ensurePerRouteCount();
			this.perRouteCount.add(new Pair<>(host, maxPerRoute));
			return this;
		}
		
		/**
		 * set userAgent
		 */
		public PooledClientBuilder userAgent(String userAgent) {
			Objects.requireNonNull(userAgent);
			this.userAgent = userAgent;
			return this;
		}
		
		/**
		 * if verify http certificate, default true
		 */
		public PooledClientBuilder verify(boolean verify) {
			this.verify = verify;
			return this;
		}
		
		/**
		 * If follow get/head redirect, default true.
		 * This method not set following redirect for post/put/delete method, use {@code allowPostRedirects} if you want this
		 */
		public PooledClientBuilder allowRedirects(boolean allowRedirects) {
			this.allowRedirects = allowRedirects;
			return this;
		}
		
		/**
		 * If follow POST/PUT/DELETE redirect, default false. This method work for post method.
		 */
		public PooledClientBuilder allowPostRedirects(boolean allowPostRedirects) {
			this.allowPostRedirects = allowPostRedirects;
			return this;
		}
		
		/**
		 * if send gzip requests. default true
		 */
		public PooledClientBuilder gzip(boolean gzip) {
			this.gzip = gzip;
			return this;
		}
		
		private void ensurePerRouteCount() {
			if (this.perRouteCount == null) {
				this.perRouteCount = new ArrayList<>();
			}
		}
		
		private PooledClientBuilder proxy(Proxy proxy) {
			this.proxy = proxy;
			return this;
		}
	}
}
