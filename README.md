Requests is a http request lib with fluent api for java, inspired by the python request module.
Requests requires JDK 1.8+, the last version support Java7 is 4.18.* .

Table of Contents
=================

* [Maven Setting](#maven-setting)
* [Usage](#usage)
  * [Simple Case](#simple-case)
  * [Charset](#charset)
  * [Passing Parameters](#passing-parameters)
  * [Set Headers](#set-headers)
  * [Cookies](#cookies)
  * [Request with data](#request-with-data)
  * [Json support](#json-support)
  * [Basic Auth](#basic-auth)
  * [Redirection](#redirection)
  * [Timeout](#timeout)
  * [Response compress](#response-compress-encoding)
  * [Https Verification](#https-verification)
  * [Proxy](#proxy)
* [Session](#session)

# Maven Setting

Requests is now in maven central repo.

```xml
<dependency>
    <groupId>net.dongliu</groupId>
    <artifactId>requests</artifactId>
    <version>5.0.8</version>
</dependency>
```

# Usage

## Simple Case
One simple http request example that do http get request and read response as string:

```java
String url = ...;
String resp = Requests.get(url).send().readToText();
// or
Response<String> resp = Requests.get(url).send().toTextResponse();
```

Post and other method:

```java
resp = Requests.post(url).send().readToText();
resp = Requests.head(url).send().readToText();
...
```

The response object have several common http response fields can be used:

```java
RawResponse resp = Requests.get(url).send();
int statusCode = resp.statusCode();
String contentLen = resp.getHeader("Content-Length");
Cookie cookie = resp.getCookie("_bd_name");
String body = resp.readToText();
```
Make sure call readToText or other methods to consume resp, or call close method to close resp.

The readToText() method here trans http response body as String, more other methods provided:

```java
// get response as string, use encoding get from response header
String resp = Requests.get(url).send().readToText();
// get response as bytes
byte[] resp1 = Requests.get(url).send().readToBytes();
// save response as file
boolean result = Requests.get(url).send().writeToFile("/path/to/save/file");
```

## Charset 

Requests default use UTF-8 to encode parameters, post forms or request string body, you can set other charset by:

```java
String resp = Requests.get(url).charset(StandardCharsets.ISO_8859_1).send().readToText();
```

When read response to text-based result, use charset get from http response header, or UTF-8 if not found.
You can force use specified charset by:

```java
String resp = Requests.get(url).send().charset(StandardCharsets.ISO_8859_1).readToText();
```

## Passing Parameters 

Pass parameters in urls using params method:

```java
// set params by map
Map<String, Object> params = new HashMap<>();
params.put("k1", "v1");
params.put("k2", "v2");
String resp = Requests.get(url).params(params).send().readToText();
// set multi params
String resp = Requests.get(url)
        .params(Parameter.of("k1", "v1"), Parameter.of("k2", "v2"))
        .send().readToText();
```

If you want to send post www-form-encoded parameters, use body() methods:

```java
// set params by map
Map<String, Object> params = new HashMap<>();
params.put("k1", "v1");
params.put("k2", "v2");
String resp = Requests.post(url).body(params).send().readToText();
// set multi params
String resp = Requests.post(url)
        .body(Parameter.of("k1", "v1"), Parameter.of("k2", "v2"))
        .send().readToText();
```
The forms parameter should only works with post method.

## Set Headers

Http request headers can be set by headers method:

```java
// set headers by map
Map<String, Object> headers = new HashMap<>();
headers.put("k1", "v1");
headers.put("k2", "v2");
String resp = Requests.get(url).headers(headers).send().readToText();
// set multi headers
String resp = Requests.get(url)
        .headers(new Header("k1", "v1"), new Header("k2", "v2"))
        .send().readToText();
```

## Cookies 

Cookies can be add by:

```java
Map<String, Object> cookies = new HashMap<>();
cookies.put("k1", "v1");
cookies.put("k2", "v2");
// set cookies by map
String resp = Requests.get(url).cookies(cookies).send().readToText();
// set cookies
String resp = Requests.get(url)
        .cookies(Parameter.of("k1", "v1"), Parameter.of("k2", "v2"))
        .send().readToText();
```

## Request with data 

Http Post, Put, Patch method can send request body. Take Post for example:

```java
// set post form data
String resp = Requests.post(url).body(Parameter.of("k1", "v1"), Parameter.of("k2", "v2"))
        .send().readToText();
// set post form data by map
Map<String, Object> formData = new HashMap<>();
formData.put("k1", "v1");
formData.put("k2", "v2");
String resp = Requests.post(url).body(formData).send().readToText();
// send byte array data as body
byte[] data = ...;
resp = Requests.post(url).body(data).send().readToText();
// send string data as body
String str = ...;
resp = Requests.post(url).body(str).send().readToText();
// send data from inputStream
InputStreamSupplier supplier = ...;
resp = Requests.post(url).body(supplier).send().readToText();
```

One more complicate situation is multiPart post request, this can be done via multiPart method, 
one simplified multi part request example which send files and param data:

```java
// send form-encoded data
InputStreamSupplier supplier = ...;
byte[] bytes = ...;
String resp = Requests.post(url)
        .multiPartBody(
            Part.file("file1", new File(...)),
            Part.file("file2", "second_file.dat", supplier),
            Part.text("input", "on")
        ).send().readToText();
```

## Json support

Requests can handle json encoder(for request body)/decoder(for response body), if having Json Binding, Jackson, Gson, or Fastjson lib in classpath.

```java
// send json body, content-type is set to application/json
RawResponse response = Requests.post("http://.../update_person")
                .jsonBody(value)
                .send();
// response body as json, to value
Person person = Requests.post("http://.../get_person")
                .params(Parameter.of("id", 101))
                .send().readToJson(Person.class);
// json body decoder to generic type
List<Person> persons = Requests.post("http://.../get_person_list")
                .send().readToJson(new TypeInfer<List<Person>>() {});

```

You may set your own json processor by:

```java
JsonProcessor jsonProcessor = ...;
JsonLookup.getInstance().register(jsonProcessor);

```

## Basic Auth 

Set http basic auth param by auth method:

```java
String resp = Requests.get(url).basicAuth("user", "passwd").send().readToText();
```

## Redirection 

Requests will handle 30x http redirect automatically, you can disable it by:

```java
Requests.get(url).followRedirect(false).send();
```

## Timeout

You can set connection connect timeout, and socket read/write timeout value, as blow:

```java
// set connect timeout and socket timeout
Requests.get(url).socksTimeout(20_000).connectTimeout(30_000).send();
```

## Response compress encoding
Requests send Accept-Encoding: gzip, deflate, and auto handle response decompress in default. You can disable this by:

```java
// do not send Accept-Encoding: gzip, deflate header
String resp = Requests.get(url).acceptCompress(false).send().readToText();
// do not decompress response body
String resp2 = Requests.get(url).send().decompress(false).readToText();
```

## Https Verification 

Some https sites do not have trusted http certificate, Exception will be thrown when request. 
You can disable https certificate verify by:

```java
Requests.get(url).verify(false).send();
```

## Proxy 

Set proxy by proxy method:

```java
Requests.get(url).proxy(Proxies.httpProxy("127.0.0.1", 8081)).send(); // http proxy
Requests.get(url).proxy(Proxies.socksProxy("127.0.0.1", 1080)).send(); // socks proxy proxy
```

# Session

Session maintains cookies, basic auth and maybe other http context for you, useful when need login or other situations. 
Session have the same usage as Requests.

```java
Session session = Requests.session();
String resp1 = session.get(url1).send().readToText();
String resp2 = session.get(url2).send().readToText();
```
