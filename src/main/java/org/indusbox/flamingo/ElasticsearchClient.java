package org.indusbox.flamingo;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Properties;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public final class ElasticsearchClient {

  private static final String MIGRATION_METADATA_MAPPING_FILE = "migration-metadata-mapping.json";
  private static final String LATEST_SCRIPT_QUERY_FILE = "latest-script-query.json";
  private static final String LATEST_SUCCESSFUL_SCRIPT_QUERY_FILE = "latest-successful-script-query.json";
  private static final String LIST_FAIL_SCRIPT_QUERY_FILE = "list-fail-script-query.json";

  private String typeName = "migration-metadata";
  private final CloseableHttpClient client;
  private final String uri;

  public ElasticsearchClient(Properties config) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
    String username = config.getProperty("elasticsearch.user");
    String password = config.getProperty("elasticsearch.password");
    String protocol = config.getProperty("elasticsearch.protocol");
    this.client = createHttpClient(username, password, protocol);
    this.uri = buildURI(config);
  }

  public JSONObject getLatestScript() throws IOException, ParseException {
    HttpPost request = createSearchRequest(LATEST_SCRIPT_QUERY_FILE);
    try (CloseableHttpResponse execute = this.client.execute(request)) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("getLatestScript, statusCode: " + statusCode);
      if (statusCode == 200) {
        String responseContent = EntityUtils.toString(execute.getEntity());
        return getFirstHit(responseContent);
      }
      throw new RuntimeException("Unable to get the latest executed script");
    }
  }

  public JSONObject getLatestSuccessfulScript() throws IOException, ParseException {
    HttpPost request = createSearchRequest(LATEST_SUCCESSFUL_SCRIPT_QUERY_FILE);
    try (CloseableHttpResponse execute = this.client.execute(request)) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("getLatestSuccessfulScript, statusCode: " + statusCode);
      if (statusCode == 200) {
        String responseContent = EntityUtils.toString(execute.getEntity());
        return getFirstHit(responseContent);
      }
      throw new RuntimeException("Unable to get the latest successful script");
    }
  }

  public JSONArray getFailScripts() throws IOException, ParseException {
    HttpPost request = createSearchRequest(LIST_FAIL_SCRIPT_QUERY_FILE);
    try (CloseableHttpResponse execute = this.client.execute(request)) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("getFailScripts, statusCode: " + statusCode);
      if (statusCode == 200) {
        JSONParser jsonParser = new JSONParser();
        JSONObject responseJson = (JSONObject) jsonParser.parse(EntityUtils.toString(execute.getEntity()));
        return (JSONArray) ((JSONObject) responseJson.get("hits")).get("hits");
      }
      throw new RuntimeException("Unable to list fail scripts");
    }
  }

  public boolean createIndex() throws IOException {
    try (CloseableHttpResponse execute = this.client.execute(new HttpPut(this.uri))) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("createIndex, statusCode: " + statusCode);
      return statusCode == 200;
    }
  }

  public boolean createType() throws IOException {
    HttpPut request = new HttpPut(this.uri + "/" + typeName + "/_mapping");
    URL url = Resources.getResource(MIGRATION_METADATA_MAPPING_FILE);
    String mapping = Resources.toString(url, Charsets.UTF_8);
    StringEntity input = new StringEntity(mapping);
    input.setContentType("application/json");
    request.setEntity(input);
    try (CloseableHttpResponse execute = this.client.execute(request)) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("createType, statusCode: " + statusCode);
      return statusCode == 200;
    }
  }

  public boolean typeExists() throws IOException {
    try (CloseableHttpResponse execute = this.client.execute(new HttpHead(this.uri + "/" + this.typeName))) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("typeExists, statusCode: " + statusCode);
      return statusCode == 200;
    }
  }

  public boolean indexExists() throws IOException {
    try (CloseableHttpResponse execute = this.client.execute(new HttpHead(this.uri))) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("indexExists, statusCode: " + statusCode);
      return statusCode == 200;
    }
  }

  public void executeScript(File script) throws IOException {
    System.out.println("Executing script " + script.getName());
    boolean succeeded = executeBulk(script);
    JSONObject scriptJson = createScriptJson(script, succeeded);
    indexScript(scriptJson);
    if (!succeeded) {
      throw new RuntimeException("Abort migration. Error while executing " + script.getName());
    }
  }

  public void updateScript(File script, String id) throws IOException {
    System.out.println("Updating script " + script.getName());
    boolean succeeded = executeBulk(script);
    JSONObject jsonObject = createScriptJson(script, succeeded);
    updateScript(jsonObject, id);
    if (!succeeded) {
      throw new RuntimeException("Abort migration. Error while updating " + script.getName());
    }
  }

  @SuppressWarnings("unchecked")
  private JSONObject createScriptJson(File script, boolean succeeded) throws IOException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("checksum", Files.hash(script, Hashing.md5()).toString());
    jsonObject.put("fileName", script.getName());
    jsonObject.put("executedDate", DateTime.now().toString(ISODateTimeFormat.dateHourMinuteSecondMillis()));
    jsonObject.put("succeeded", succeeded);
    return jsonObject;
  }

  public boolean executeBulk(File scriptFile) throws IOException {
    HttpPost bulkRequest = createBulkRequest(scriptFile);
    try (CloseableHttpResponse execute = this.client.execute(bulkRequest)) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("executeBulk, statusCode: " + statusCode);
      String result = EntityUtils.toString(execute.getEntity());
      System.out.println("executeBulk, result: " + result);
      return statusCode == 200;
    }
  }

  public boolean indexScript(JSONObject scriptJson) throws IOException {
    HttpPost request = new HttpPost(this.uri + "/" + this.typeName);
    StringEntity entity = new StringEntity(scriptJson.toJSONString());
    entity.setContentType("application/json");
    request.setEntity(entity);
    try (CloseableHttpResponse execute = this.client.execute(request)) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("indexScript, statusCode: " + statusCode);
      String result = EntityUtils.toString(execute.getEntity());
      System.out.println("indexScript, result: " + result);
      return statusCode == 200;
    }
  }

  public boolean updateScript(JSONObject scriptJson, String id) throws IOException {
    HttpPut request = new HttpPut(this.uri + "/" + this.typeName + "/" + id);
    StringEntity entity = new StringEntity(scriptJson.toJSONString());
    entity.setContentType("application/json");
    request.setEntity(entity);
    try (CloseableHttpResponse execute = this.client.execute(request)) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("updateScript, statusCode: " + statusCode);
      String result = EntityUtils.toString(execute.getEntity());
      System.out.println("updateScript, result: " + result);
      return statusCode == 200;
    }
  }

  public Long count() throws IOException, ParseException {
    try (CloseableHttpResponse execute = this.client.execute(new HttpGet(this.uri + "/" + this.typeName + "/_count"))) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("count, statusCode: " + statusCode);
      if (statusCode == 200) {
        JSONParser jsonParser = new JSONParser();
        JSONObject responseJson = (JSONObject) jsonParser.parse(EntityUtils.toString(execute.getEntity()));
        return (Long) responseJson.get("count");
      }
      throw new RuntimeException("Unable to count executed scripts");
    }
  }

  public JSONArray list(Long size) throws IOException, ParseException {
    try (CloseableHttpResponse execute = this.client.execute(new HttpGet(this.uri + "/" + this.typeName + "/_search?size=" + size))) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("count, statusCode: " + statusCode);
      if (statusCode == 200) {
        JSONParser jsonParser = new JSONParser();
        JSONObject responseJson = (JSONObject) jsonParser.parse(EntityUtils.toString(execute.getEntity()));
        return (JSONArray) ((JSONObject) responseJson.get("hits")).get("hits");
      }
      throw new RuntimeException("Unable to count executed scripts");
    }
  }

  public JSONObject searchScript(String fileName) throws IOException, ParseException {
    try (CloseableHttpResponse execute = this.client.execute(new HttpGet(this.uri + "/" + this.typeName + "/_search?q=fileName:" + fileName))) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("searchScript, statusCode: " + statusCode);
      if (statusCode == 200) {
        String responseContent = EntityUtils.toString(execute.getEntity());
        return getFirstHit(responseContent);
      }
      throw null;
    }
  }

  public static String buildURI(Properties config) {
    String esProtocol = config.getProperty("elasticsearch.protocol");
    String esHost = config.getProperty("elasticsearch.host");
    String esIndexName = config.getProperty("elasticsearch.index");
    String esPort = config.getProperty("elasticsearch.port");
    return esProtocol + "://" + esHost + ":" + esPort + "/" + esIndexName;
  }

  public static CloseableHttpClient createHttpClient(String username, String password, String protocol) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
    HttpClientBuilder httpClientBuilder = HttpClients.custom();
    if ("https".equals(protocol)) {
      SSLContextBuilder builder = new SSLContextBuilder();
      builder.loadTrustMaterial(null, new TrustStrategy() {
        @Override
        public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
          return true;
        }
      });
      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());
      httpClientBuilder.setSSLSocketFactory(sslsf);
    }
    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
    httpClientBuilder.setDefaultCredentialsProvider(credsProvider);
    return httpClientBuilder.build();
  }

  private JSONObject getFirstHit(String responseContent) throws ParseException {
    JSONParser jsonParser = new JSONParser();
    JSONObject jsonObject = (JSONObject) jsonParser.parse(responseContent);
    JSONArray hits = (JSONArray) ((JSONObject) jsonObject.get("hits")).get("hits");
    if (hits.isEmpty()) {
      return null;
    }
    return (JSONObject) hits.get(0);
  }

  private HttpPost createSearchRequest(String queryFile) throws IOException {
    HttpPost request = new HttpPost(this.uri + "/" + this.typeName + "/_search");
    return createRequestFormResource(queryFile, request);
  }

  private HttpPost createBulkRequest(File scriptFile) throws IOException {
    HttpPost request = new HttpPost(this.uri + "/_bulk");
    return createRequestFormFile(scriptFile, request);
  }

  private HttpPost createRequestFormFile(File scriptFile, HttpPost request) throws IOException {
    String jsonContent = Files.toString(scriptFile, Charsets.UTF_8);
    request.setEntity(createJsonEntity(jsonContent));
    return request;
  }

  private HttpPost createRequestFormResource(String scriptFile, HttpPost request) throws IOException {
    URL url = Resources.getResource(scriptFile);
    String jsonContent = Resources.toString(url, Charsets.UTF_8);
    request.setEntity(createJsonEntity(jsonContent));
    return request;
  }

  private StringEntity createJsonEntity(String jsonContent) throws UnsupportedEncodingException {
    StringEntity jsonEntity = new StringEntity(jsonContent);
    jsonEntity.setContentType("application/json");
    return jsonEntity;
  }
}
