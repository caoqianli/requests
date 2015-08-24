package net.dongliu.requests;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.dongliu.requests.async.Callback;
import net.dongliu.requests.exception.RequestException;
import net.dongliu.requests.struct.Cookie;
import net.dongliu.requests.struct.Cookies;
import net.dongliu.requests.struct.Header;
import net.dongliu.requests.struct.Headers;
import net.dongliu.requests.struct.MultiPart;
import net.dongliu.requests.struct.Parameter;
import net.dongliu.requests.struct.Proxy;
import org.apache.commons.io.Charsets;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CookieStore;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.Registry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.nio.client.util.HttpAsyncClientUtils;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.reactor.IOReactorException;

/**
 * Execute request and get response result.
 *
 * @param <T> the response body type
 * @author Dong Liu dongliu@wandoujia.com
 */
class RequestExecutor<T> {
	private final Request request;
	private final ResponseProcessor<T> processor;
	
	private final Session session;
	private final PooledClient pooledClient;
	
	RequestExecutor(Request request, ResponseProcessor<T> processor, Session session, PooledClient pooledClient) {
		this.request = request;
		this.processor = processor;
		this.session = session;
		this.pooledClient = pooledClient;
	}
	
	/**
	 * execute request, get http response, and convert response with processor
	 */
	Response<T> execute() throws IOException {
		CredentialsProvider provider = new BasicCredentialsProvider();
		HttpClientContext context;
		if (session != null) {
			context = session.getContext();
		} else {
			context = HttpClientContext.create();
			CookieStore cookieStore = new BasicCookieStore();
			context.setCookieStore(cookieStore);
		}
		
		HttpRequestBase httpRequest = buildRequest(provider, context);
		// basic auth
		if (request.getAuthInfo() != null) {
			UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(request.getAuthInfo().getUserName(),
			                                                                          request.getAuthInfo().getPassword());
			provider.setCredentials(new AuthScope(request.getUrl().getHost(), request.getUrl().getPort()), credentials);
		}
		
		context.setAttribute(HttpClientContext.CREDS_PROVIDER, provider);
		CloseableHttpClient client = null;
		try {
			client = buildHttpClient();
			// do http request with http client
			try (CloseableHttpResponse httpResponse = client.execute(httpRequest, context)) {
				return wrapResponse(httpResponse, context);
			}
		} finally {
			if (pooledClient == null && client != null) {
				// we create new single client for this request
				try {
					client.close();
				} catch (IOException ignore) {
				}
			}
		}
	}
	
	Future<Response<T>> execute(Callback<T> callback) throws IOException {
		CredentialsProvider provider = new BasicCredentialsProvider();
		final HttpClientContext context;
		if (session != null) {
			context = session.getContext();
		} else {
			context = HttpClientContext.create();
			CookieStore cookieStore = new BasicCookieStore();
			context.setCookieStore(cookieStore);
		}
		
		HttpRequestBase httpRequest = buildRequest(provider, context);
		// basic auth
		if (request.getAuthInfo() != null) {
			UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(request.getAuthInfo().getUserName(),
			                                                                          request.getAuthInfo().getPassword());
			provider.setCredentials(new AuthScope(request.getUrl().getHost(), request.getUrl().getPort()), credentials);
		}
		
		context.setAttribute(HttpClientContext.CREDS_PROVIDER, provider);
		
		CloseableHttpAsyncClient httpAsyncClient = buildHttpAsyncClient();
		if (!httpAsyncClient.isRunning()) {
			httpAsyncClient.start();
		}
		final Future<org.apache.http.HttpResponse> future = httpAsyncClient.execute(httpRequest,
		                                                                            context,
		                                                                            prepareCallback(httpAsyncClient, context, callback));
		return new Future<Response<T>>() {
			
			public boolean cancel(boolean mayInterruptIfRunning) {
				return future.cancel(mayInterruptIfRunning);
			}
			
			public boolean isCancelled() {
				return future.isCancelled();
			}
			
			public boolean isDone() {
				return future.isDone();
			}
			
			public Response<T> get() throws InterruptedException, ExecutionException {
				HttpResponse httpResponse = future.get();
				try {
					return wrapResponse(httpResponse, context);
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
			
			public Response<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
			                                                           TimeoutException {
				HttpResponse httpResponse = future.get(timeout, unit);
				try {
					return wrapResponse(httpResponse, context);
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
		};
	}
	
	private FutureCallback<HttpResponse> prepareCallback(final CloseableHttpAsyncClient httpAsyncClient,
	                                                         final HttpClientContext context,
	                                                         final Callback<T> callback) {
		if (callback == null) {
			return null;
		}
		
		return new FutureCallback<HttpResponse>() {
			
			public void cancelled() {
				try {
					callback.cancelled();
				} finally {
					if (pooledClient == null) {
						HttpAsyncClientUtils.closeQuietly(httpAsyncClient);
					}
				}
			}
			
			public void completed(HttpResponse httpResponse) {
				try {
					callback.completed(wrapResponse(httpResponse, context));
				} catch (IOException e) {
					callback.failed(new RequestException(e));
				} finally {
					if (pooledClient == null) {
						HttpAsyncClientUtils.closeQuietly(httpAsyncClient);
					}
				}
			}
			
			public void failed(Exception e) {
				try {
					callback.failed(new RequestException(e));
				} finally {
					if (pooledClient == null) {
						HttpAsyncClientUtils.closeQuietly(httpAsyncClient);
					}
				}
			}
			
		};
	}
	
	/**
	 * build http client
	 */
	private CloseableHttpClient buildHttpClient() {
		if (pooledClient != null) {
			return pooledClient.getHttpClient();
		}
		HttpClientBuilder clientBuilder = HttpClients.custom().setUserAgent(request.getUserAgent());
		
		Registry<ConnectionSocketFactory> reg = Utils.getConnectionSocketFactoryRegistry(request.getProxy(), request.isVerify());
		BasicHttpClientConnectionManager manager = new BasicHttpClientConnectionManager(reg);
		clientBuilder.setConnectionManager(manager);
		
		// disable gzip
		if (!request.isGzip()) {
			clientBuilder.disableContentCompression();
		}
		
		// get response
		if (!request.isAllowRedirects()) {
			clientBuilder.disableRedirectHandling();
		}
		
		if (request.isAllowPostRedirects()) {
			clientBuilder.setRedirectStrategy(new AllRedirectStrategy());
		}
		
		return clientBuilder.build();
	}
	
	private CloseableHttpAsyncClient buildHttpAsyncClient() throws IOReactorException {
		if (pooledClient != null) {
			return pooledClient.getHttpAsyncClient();
		}
		HttpAsyncClientBuilder asyncClientBuilder = HttpAsyncClientBuilder.create().setUserAgent(request.getUserAgent());
		
		Registry<SchemeIOSessionStrategy> reg = Utils.getSchemeIOSessionStrategy(request.isVerify());
		PoolingNHttpClientConnectionManager manager = new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor(), reg);
		asyncClientBuilder.setConnectionManager(manager);
		
		if (request.isAllowPostRedirects()) {
			asyncClientBuilder.setRedirectStrategy(new AllRedirectStrategy());
		}
		
		return asyncClientBuilder.build();
	}
	
	// build http request
	private HttpRequestBase buildRequest(CredentialsProvider provider, HttpClientContext context) {
		URI uri = Utils.fullUrl(request.getUrl(), request.getCharset(), request.getParameters());
		HttpRequestBase httpRequest;
		switch (request.getMethod()) {
		case POST:
			httpRequest = buildHttpPost(uri, request);
			break;
		case GET:
			httpRequest = new HttpGet(uri);
			break;
		case HEAD:
			httpRequest = new HttpHead(uri);
			break;
		case PUT:
			httpRequest = buildHttpPut(uri, request);
			break;
		case DELETE:
			httpRequest = new HttpDelete(uri);
			break;
		case OPTIONS:
			httpRequest = new HttpOptions(uri);
			break;
		case TRACE:
			httpRequest = new HttpTrace(uri);
			break;
		case PATCH:
			httpRequest = buildHttpPatch(uri, request);
			break;
		case CONNECT:
		default:
			throw new UnsupportedOperationException("Unsupported method:" + request.getMethod());
		}
		
		RequestConfig.Builder configBuilder = RequestConfig.custom()
		                                                   .setConnectTimeout(request.getConnectTimeout())
				                                      .setSocketTimeout(request.getSocketTimeout())
						                                       // we use connect timeout for connection request timeout
				                                      .setConnectionRequestTimeout(request.getConnectTimeout())
				                                      .setCookieSpec(CookieSpecs.DEFAULT);
		
		//proxy. connection proxy settings override request proxy
		Proxy proxy = pooledClient == null ? request.getProxy() : pooledClient.getProxy();
		if (proxy != null && (proxy.getScheme() == Proxy.Scheme.http
				                      || proxy.getScheme() == Proxy.Scheme.https)) {
			//http or https proxy
			if (proxy.getAuthInfo() != null) {
				provider.setCredentials(new AuthScope(proxy.getHost(), proxy.getPort()),
				                        new UsernamePasswordCredentials(proxy.getUserName(), proxy.getPassword()));
			}
			HttpHost httpHost = new HttpHost(proxy.getHost(), proxy.getPort(),
			                                 proxy.getScheme().name());
			configBuilder.setProxy(httpHost);
		}
		httpRequest.setConfig(configBuilder.build());
		
		// set cookie
		CookieStore cookieStore = context.getCookieStore();
		if (request.getCookies() != null) {
			for (Cookie cookie : request.getCookies()) {
				BasicClientCookie clientCookie = new BasicClientCookie(cookie.getName(),
				                                                       cookie.getValue());
				clientCookie.setDomain(request.getUrl().getHost());
				clientCookie.setPath("/");
				cookieStore.addCookie(clientCookie);
			}
		}
		
		// set headers
		if (request.getHeaders() != null) {
			for (Header header : request.getHeaders()) {
				httpRequest.setHeader(header.getName(), header.getValue());
			}
		}
		
		return httpRequest;
	}
	
	private HttpRequestBase buildHttpPut(URI uri, Request request) {
		Utils.checkHttpBody(request);
		HttpPut httpPut = new HttpPut(uri);
		if (request.getStrBody() != null) {
			httpPut.setEntity(new StringEntity(request.getStrBody(), request.getCharset()));
		} else if (request.getBody() != null) {
			httpPut.setEntity(new ByteArrayEntity(request.getBody()));
		} else if (request.getIn() != null) {
			httpPut.setEntity(new InputStreamEntity(request.getIn()));
		} else if (request.getParamBody() != null) {
			// use www-form-urlencoded to send params
			List<BasicNameValuePair> paramList = new ArrayList<>(request.getParamBody().size());
			for (Parameter param : request.getParamBody()) {
				paramList.add(new BasicNameValuePair(param.getName(), param.getValue()));
			}
			UrlEncodedFormEntity entity = new UrlEncodedFormEntity(paramList, Charsets.UTF_8);
			httpPut.setEntity(entity);
		}
		return httpPut;
	}
	
	private HttpPost buildHttpPost(URI uri, Request request) {
		Utils.checkHttpBody(request);
		
		HttpPost httpPost = new HttpPost(uri);
		if (request.getMultiParts() != null) {
			MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
			for (MultiPart f : request.getMultiParts()) {
				switch (f.getType()) {
				case TEXT:
					entityBuilder.addTextBody(f.getName(), f.getValue());
					break;
				case FILE:
					entityBuilder.addBinaryBody(f.getName(), f.getFile(),
					                            ContentType.create(f.getMime()), f.getFileName());
					break;
				case STREAM:
					entityBuilder.addBinaryBody(f.getName(), f.getIn(),
					                            ContentType.create(f.getMime()), f.getFileName());
					break;
				case BYTES:
					entityBuilder.addBinaryBody(f.getName(), f.getBytes(),
					                            ContentType.create(f.getMime()), f.getFileName());
					break;
				}
				
			}
			httpPost.setEntity(entityBuilder.build());
		} else if (request.getStrBody() != null) {
			httpPost.setEntity(new StringEntity(request.getStrBody(), request.getCharset()));
		} else if (request.getBody() != null) {
			httpPost.setEntity(new ByteArrayEntity(request.getBody()));
		} else if (request.getIn() != null) {
			httpPost.setEntity(new InputStreamEntity(request.getIn()));
		} else if (request.getParamBody() != null) {
			// use www-form-urlencoded to send params
			List<BasicNameValuePair> paramList = new ArrayList<>(request.getParamBody().size());
			for (Parameter param : request.getParamBody()) {
				paramList.add(new BasicNameValuePair(param.getName(), param.getValue()));
			}
			UrlEncodedFormEntity entity = new UrlEncodedFormEntity(paramList, request.getCharset());
			httpPost.setEntity(entity);
		}
		return httpPost;
	}
	
	private HttpRequestBase buildHttpPatch(URI uri, Request request) {
		Utils.checkHttpBody(request);
		HttpPatch httpPatch = new HttpPatch(uri);
		if (request.getStrBody() != null) {
			httpPatch.setEntity(new StringEntity(request.getStrBody(), request.getCharset()));
		} else if (request.getBody() != null) {
			httpPatch.setEntity(new ByteArrayEntity(request.getBody()));
		} else if (request.getIn() != null) {
			httpPatch.setEntity(new InputStreamEntity(request.getIn()));
		} else if (request.getParamBody() != null) {
			// use www-form-urlencoded to send params
			List<BasicNameValuePair> paramList = new ArrayList<>(request.getParamBody().size());
			for (Parameter param : request.getParamBody()) {
				paramList.add(new BasicNameValuePair(param.getName(), param.getValue()));
			}
			UrlEncodedFormEntity entity = new UrlEncodedFormEntity(paramList, Charsets.UTF_8);
			httpPatch.setEntity(entity);
		}
		return httpPatch;
	}
	
	/**
	 * do http request with http client
	 */
	private Response<T> wrapResponse(HttpResponse httpResponse,
	                                 HttpClientContext context) throws IOException {
		Response<T> response = new Response<>();
		response.setStatusCode(httpResponse.getStatusLine().getStatusCode());
		// get headers
		org.apache.http.Header[] respHeaders = httpResponse.getAllHeaders();
		Headers headers = new Headers();
		for (org.apache.http.Header header : respHeaders) {
			headers.add(new Header(header.getName(), header.getValue()));
		}
		response.setHeaders(headers);
		
		// get cookies
		Cookies cookies = new Cookies();
		for (org.apache.http.cookie.Cookie c : context.getCookieStore().getCookies()) {
			Cookie cookie = new Cookie(c.getName(), c.getValue());
			cookie.setPath(c.getPath());
			cookie.setDomain(c.getDomain());
			cookie.setPath(c.getPath());
			cookie.setExpiry(c.getExpiryDate());
			cookies.add(cookie);
		}
		response.setCookies(cookies);
		
		response.setHistory(context.getRedirectLocations());
		
		HttpEntity entity = httpResponse.getEntity();
		T result = processor.convert(response.getStatusCode(), headers, entity);
		response.setBody(result);
		return response;
	}
	
}
