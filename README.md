#Requests

Requests is a http request lib for java inspired by The Python [requests](http://docs.python-requests.org/) Module, using HttpClient as backend and with fluent api.

The HttpClient lib is great, but has too complex API, which confuse beginners. Requests build simple and flexible api, both for common and advanced Usage.

#User Guide
##Get Requests
Requests is now in maven central repo.
```xml
<dependency>
    <groupId>net.dongliu</groupId>
    <artifactId>requests</artifactId>
    <version>2.0.0</version>
</dependency>
```
##Make request
Simple example that do http get request:
```java
String url = ...;
Response<String> resp = Requests.get(url).text();
```
Post and other method:
```java
resp = Requests.post(url).text();
resp = Requests.head(url).text();
...
```
##Response Content
The response object have common http response field to be used:
```java
Response<String> resp = Requests.get(url).text();
int statusCode = resp.getStatusCode();
Headers headers = resp.getHeaders();
Cookies cookies = resp.getCookies();
String body = resp.getBody();
```
The text() method here trans http response body as String, there are other method to process http response:
```java
// get response as string, use encoding get from response header
Response<String> resp = Requests.get(url).text();
// get response as string, and use provided encoding
Response<String> resp = Requests.get(url).text(StandardCharsets.UTF-8);
// get response as bytes
Response<byte[]> resp1 = Requests.get(url).bytes();
// save response as file 
Response<File> resp2 = Requests.get(url).file("/path/to/save/file");
```
or you can custom http response processor your self:
```java
Response<String> resp = Requests.get(url).handle(new ResponseHandler<String>() {...});
```
##Request Charset
Set charset used to encode parameters, post forms or request string body:
```
Response<String> resp = Requests.get(url).charset(StandardCharsets.UTF_8).text();
```
Default charset is utf-8.

##Passing Parameters
Pass parameters in urls using param or params method:
```java
Response<String> resp = Requests.get(url)
        // add one param
        .addParam("key1", "value1")
        .addParam("key1", "value1")
        .text();
// set params by map
Map<String, Object> params = new HashMap<>();
params.put("k1", "v1");
params.put("k2", "v2");
Response<String> resp = Requests.get(url).params(params).text();
// set multi params
Response<String> resp = Requests.get(url).params(new Parameter(...), new Parameter(...))
        .text();
```
If you want to send post form-encoded paramters, use form()/forms() methods
##Custom Headers
Http request headers can be set by header or headers method:
```java
Response<String> resp = Requests.get(url)
        // add one header
        .addHeader("key1", "value1")
        .addHeader("key2", "value2")
        .text();
// set multi headers by map
Map<String, Object> headers = new HashMap<>();
headers.put("k1", "v1");
headers.put("k2", "v2");
Response<String> resp = Requests.get(url).headers(headers).text();
// set multi headers
Response<String> resp = Requests.get(url).headers(new Header(...), new Header(...))
        .text();
```
##Cookies
Cookies can be add by:
```java
Response<String> resp = Requests.get(url)
        // add one cookie
        .addCookie("key1", "value1")
        .addCookie("key2", "value2")
        .text();
Map<String, Object> cookies = new HashMap<>();
cookies.put("k1", "v1");
cookies.put("k2", "v2");
// set cookies by map
Response<String> resp = Requests.get(url).cookies(cookies).text();
// set cookies
Response<String> resp = Requests.get(url).cookies(new Cookie(...), new Cookie(...))
        .text();
```
##UserAgent
A convenient method to set userAgent:
```java
Response<String> resp = Requests.get(url).userAgent(userAgent).text();
```
##Request with data
Http Post, Put, Patch method can send request body. Take Post for example:
```java
// add post from data
Response<String> resp = Requests.post(url)
        .addForm("key1", "value1").addForm("key2", "value2").text();
// set post form data
Response<String> resp = Requests.post(url).forms(new Parameter(...), new Parameter(...))
        .text();
// set post form data by map
Map<String, Object> formData = new HashMap<>();
formData.put("k1", "v1");
formData.put("k2", "v2");
Response<String> resp = Requests.post(url).forms(formData).text();
// send byte array data as body
byte[] data = ...;
resp = Requests.post(url).data(data).text();
// send string data as body
String str = ...;
resp = Requests.post(url).data(str).text();
// send data from inputStream
InputStream in = ...
resp = Requests.post(url).data(in).text();
```
One more complicate situation is multiPart post request, this can be done via multiPart method:
```java
// send form-encoded data
InputStream in = ...;
byte[] bytes = ...;
Response<String> resp = Requests.post(url)
        .addMultiPart("test", "value")
        .addMultiPart("byFile", new File("/path/to/file"))
        .addMultiPart("byStream", mimeType, in)
        .addMultiPart("byBytes", mimeType, bytes)
        .text();
```
###Redirection
Requests will handle 301/302 http redirect, you can get redirect history:
```java
Response<String> resp = Requests.get(url).text();
List<URI> history = resp.getHistory();
```
Or you can disable automatic redirect and handle it yourself:
```java
Response<String> resp = Requests.get(url).allowRedirects(false).text();
```
## Timeout
There are two timeout parameters you can set, connect timeout, and socket timeout. The timeout value default to 10_1000 milliseconds.
```java
// both connec timeout, and socket timeout
Response<String> resp = Requests.get(url).timeout(30_000).text();
// set connect timeout and socket timeout
resp = Requests.get(url).timeout(30_000, 30_000).text();
```
You may not need to know, but Requests also use connect timeout as the timeout value get connection from connection pool if connection pool is used.
##Gzip
Requests send Accept-Encoding: gzip, deflate, and handle gzipped response in default. You can disable this by:
```java
Response<String> resp = Requests.get(url).compress(false).text();
```
##Https Verification
Some https sites do not have trusted http certificate, Exception will be throwed when request. You can disable https certificate verify by:
```java
Response<String> resp = Requests.get(url).verify(false).text();
```
##Basic Auth
Set http basic auth param by auth method:
```java
Response<String> resp = Requests.get(url).auth("user", "passwd").verify(false).text();
```
##Proxy
Set proxy by proxy method:
```java
Response<String> resp = Requests.get("http://www.baidu.com/")
        .proxy(Proxy.httpProxy("127.0.0.1", 8080))
        .text();
```
The proxy can be created by:
```java
//http proxy
Proxy.httpProxy("127.0.0.1", 8080)
//https proxy
Proxy.httpsProxy("127.0.0.1", 8080)
//socket proxy
Proxy.socketProxy("127.0.0.1", 5678)
//with auth
Proxy.httpProxy("127.0.0.1", 8080, userName, password)
```
##Exceptions
Requests wrapped all checked exceptions into one runtime exception: RequestException. Catch this if you mind.Unchecked Exceptions are leaved as it is.
## Session
Session keep cookies and basic auto cache and other http context for you, useful when need login or other situations.Session have exactly the same usage as Requests.
```java
Session session = Requests.session();
Response<String> resp1 = session.get(url1).text();
Response<String> resp2 = session.get(url2).text();
```
##Connection Pool
Request(and Session) can share one connection pool to reuse http connections, via PooledClient.
```java
Client client = Client.custom().verify(false)
       .maxPerRoute(20)
       .maxTotal(100)
       .proxy(...)
       .verify(false)
       .userAgent(...)
       .build();
try {
    Response<String> resp1 = client.get(url1).text();
    Response<String> resp2 = client.get(url2).text();

    // get session
    Session session = client.session();
    Response<String> response = session.get(url3).text();

} finally {
    // close
    client.close();
}
```
Note:
* You should make sure PooledClient be closed when do not need it any more.
* If connection pool is used, you should set verify and proxy use ConnectionPoolBuilder, the connection pool's (verify, proxy) settings will override requests' settings.
* Settings: userAgent, compress, allowRedirects should be set at client level, request level settings will not work due to implementation restrict now.
