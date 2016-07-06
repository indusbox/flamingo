package org.indusbox.flamingo.scripts;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

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
import org.indusbox.flamingo.settings.FlamingoSettings;
import org.indusbox.flamingo.utils.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public final class ScriptManager {

  private static final String FLAMINGO_INDEX_FILE = "flamingo-index.json";
  private static final String LATEST_SCRIPT_QUERY_FILE = "latest-script-query.json";
  private static final String LATEST_SUCCESSFUL_SCRIPT_QUERY_FILE = "latest-successful-script-query.json";
  private static final String LIST_FAIL_SCRIPT_QUERY_FILE = "list-fail-script-query.json";

  private final CloseableHttpClient client;
  private final String uri;
  private final String flamingoIndexName;
  private final String flamingoTypeName = "migration-metadata";

  public ScriptManager(FlamingoSettings settings) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
    this.client = createHttpClient(settings);
    this.uri = buildURI(settings);
    this.flamingoIndexName = settings.getIndexName();
  }

  public ScriptMetadata getLatestScript() throws IOException {
    HttpPost request = createSearchRequest(LATEST_SCRIPT_QUERY_FILE);
    try (CloseableHttpResponse execute = this.client.execute(request)) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("getLatestScript, statusCode: " + statusCode);
      if (statusCode == 200) {
        String responseContent = EntityUtils.toString(execute.getEntity());
        return ScriptMetadata.fromJSON(getFirstHit(responseContent));
      }
      throw new RuntimeException("Unable to get the latest executed script");
    }
  }

  public ScriptMetadata getLatestSuccessfulScript() throws IOException {
    HttpPost request = createSearchRequest(LATEST_SUCCESSFUL_SCRIPT_QUERY_FILE);
    try (CloseableHttpResponse execute = this.client.execute(request)) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("getLatestSuccessfulScript, statusCode: " + statusCode);
      if (statusCode == 200) {
        String responseContent = EntityUtils.toString(execute.getEntity());
        return ScriptMetadata.fromJSON(getFirstHit(responseContent));
      }
      throw new RuntimeException("Unable to get the latest successful script");
    }
  }

  public List<ScriptMetadata> getFailScripts() throws IOException {
    HttpPost request = createSearchRequest(LIST_FAIL_SCRIPT_QUERY_FILE);
    try (CloseableHttpResponse execute = this.client.execute(request)) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("getFailScripts, statusCode: " + statusCode);
      if (statusCode == 200) {
        JSONParser jsonParser = new JSONParser();
        JSONObject responseJson = (JSONObject) jsonParser.parse(EntityUtils.toString(execute.getEntity()));
        List<ScriptMetadata> result = new ArrayList<>();
        JSONArray hits = (JSONArray) ((JSONObject) responseJson.get("hits")).get("hits");
        for (Object hit : hits) {
          JSONObject hitJSON = (JSONObject) hit;
          result.add(ScriptMetadata.fromJSON(hitJSON));
        }
        return result;
      }
      throw new RuntimeException("Unable to list fail scripts");
    } catch (ParseException e) {
      throw new RuntimeException("Unable to parse result", e);
    }
  }

  public boolean createFlamingoIndex() throws IOException {
    HttpPut request = new HttpPut(this.uri + "/" + this.flamingoIndexName);
    URL url = Resources.getResource(FLAMINGO_INDEX_FILE);
    String mapping = Resources.toString(url, Charsets.UTF_8);
    StringEntity input = new StringEntity(mapping);
    input.setContentType("application/json");
    request.setEntity(input);
    try (CloseableHttpResponse response = this.client.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      System.out.println("createIndex, statusCode: " + statusCode);
      // Wait index creation, cluster status must be at least yellow
      HttpGet healthRequest = new HttpGet(this.uri + "/_cluster/health?wait_for_status=yellow");
      try (CloseableHttpResponse healthResponse = this.client.execute(healthRequest)) {
        int healthStatusCode = healthResponse.getStatusLine().getStatusCode();
        System.out.println("clusterHealth, statusCode: " + statusCode);
        return healthStatusCode == 200;
      }
    }
  }

  public boolean indexFlamingoExists() throws IOException {
    try (CloseableHttpResponse execute = this.client.execute(new HttpHead(this.uri + "/" + this.flamingoIndexName))) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("indexExists, statusCode: " + statusCode);
      return statusCode == 200;
    }
  }

  public void executeScript(File script, File baseDir) throws IOException {
    System.out.println("Executing script " + script.getName());
    boolean succeeded = executeBulk(script);
    JSONObject scriptJson = createScriptJson(script, baseDir, succeeded);
    indexScript(scriptJson);
    if (!succeeded) {
      throw new RuntimeException("Abort migration. Error while executing " + script.getName());
    }
  }

  public void updateScript(File script, File baseDir, String id) throws IOException {
    System.out.println("Updating script " + script.getName());
    boolean succeeded = executeBulk(script);
    JSONObject jsonObject = createScriptJson(script, baseDir, succeeded);
    updateScript(jsonObject, id);
    if (!succeeded) {
      throw new RuntimeException("Abort migration. Error while updating " + script.getName());
    }
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
    HttpPost request = new HttpPost(this.uri + "/" + this.flamingoIndexName + "/" + this.flamingoTypeName + "?refresh=true");
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

  public Long count() throws IOException {
    try (CloseableHttpResponse execute = this.client.execute(new HttpGet(this.uri + "/" + this.flamingoIndexName + "/" + this.flamingoTypeName + "/_search?size=0"))) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("count, statusCode: " + statusCode);
      if (statusCode == 200) {
        JSONParser jsonParser = new JSONParser();
        JSONObject responseJson = (JSONObject) jsonParser.parse(EntityUtils.toString(execute.getEntity()));
        return (Long) ((JSONObject) responseJson.get("hits")).get("total");
      }
      System.out.println("count, response: " + EntityUtils.toString(execute.getEntity()));
      throw new RuntimeException("Unable to count executed scripts");
    } catch (ParseException e) {
      throw new RuntimeException("Unable to parse result", e);
    }
  }

  public List<ScriptMetadata> list(Long size) throws IOException {
    try (CloseableHttpResponse execute = this.client.execute(new HttpGet(this.uri + "/" + this.flamingoIndexName + "/" + this.flamingoTypeName + "/_search?size=" + size + "&sort=fileName:asc"))) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("count, statusCode: " + statusCode);
      if (statusCode == 200) {
        JSONParser jsonParser = new JSONParser();
        JSONObject responseJson = (JSONObject) jsonParser.parse(EntityUtils.toString(execute.getEntity()));
        JSONArray hits = (JSONArray) ((JSONObject) responseJson.get("hits")).get("hits");
        List<ScriptMetadata> result = new ArrayList<>();
        for (Object hit : hits) {
          JSONObject hitJSON = (JSONObject) hit;
          result.add(ScriptMetadata.fromJSON(hitJSON));
        }
        return result;
      }
      throw new RuntimeException("Unable to count executed scripts");
    } catch (ParseException e) {
      throw new RuntimeException("Unable to parse result", e);
    }
  }

  public boolean scriptExists(String fileName) throws IOException {
    return searchScript(fileName) != null;
  }

  private boolean updateScript(JSONObject scriptJson, String id) throws IOException {
    HttpPut request = new HttpPut(this.uri + "/" + this.flamingoIndexName + "/" + this.flamingoTypeName + "/" + id + "?refresh=true");
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

  private JSONObject searchScript(String fileName) throws IOException {
    try (CloseableHttpResponse execute = this.client.execute(new HttpGet(this.uri + "/" + this.flamingoIndexName + "/" + this.flamingoTypeName + "/_search?q=fileName:" + fileName))) {
      int statusCode = execute.getStatusLine().getStatusCode();
      System.out.println("searchScript, statusCode: " + statusCode);
      if (statusCode == 200) {
        String responseContent = EntityUtils.toString(execute.getEntity());
        return getFirstHit(responseContent);
      }
      throw null;
    }
  }

  private String buildURI(FlamingoSettings settings) {
    String esProtocol = settings.getProtocol();
    String esHost = settings.getHostName();
    int esPort = settings.getPort();
    return esProtocol + "://" + esHost + ":" + esPort;
  }

  private CloseableHttpClient createHttpClient(FlamingoSettings settings) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
    HttpClientBuilder httpClientBuilder = HttpClients.custom();
    String username = settings.getUsername();
    String password = settings.getPassword();
    String protocol = settings.getProtocol();
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
    if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
      httpClientBuilder.setDefaultCredentialsProvider(credsProvider);
    }
    return httpClientBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private JSONObject createScriptJson(File script, File baseDir, boolean succeeded) throws IOException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("checksum", Files.hash(script, Hashing.md5()).toString());
    final String scriptName =  StringUtils.removeStart(script.getCanonicalPath(), baseDir.getCanonicalPath() + File.separator).replaceAll("\\\\", "/");
    jsonObject.put("fileName", scriptName);
    jsonObject.put("executedDate", DateTime.now().toString(ISODateTimeFormat.dateHourMinuteSecondMillis()));
    jsonObject.put("succeeded", succeeded);
    return jsonObject;
  }

  private JSONObject getFirstHit(String responseContent) {
    try {
      JSONParser jsonParser = new JSONParser();
      JSONObject jsonObject = (JSONObject) jsonParser.parse(responseContent);
      JSONArray hits = (JSONArray) ((JSONObject) jsonObject.get("hits")).get("hits");
      if (hits.isEmpty()) {
        return null;
      }
      return (JSONObject) hits.get(0);
    } catch (ParseException e) {
      throw new RuntimeException("Unable to parse result", e);
    }
  }

  private HttpPost createSearchRequest(String queryFile) throws IOException {
    HttpPost request = new HttpPost(this.uri + "/" + this.flamingoIndexName + "/" + this.flamingoTypeName + "/_search");
    return createRequestFormResource(queryFile, request);
  }

  private HttpPost createBulkRequest(File scriptFile) throws IOException {
    HttpPost request = new HttpPost(this.uri + "/_bulk?refresh=true");
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
