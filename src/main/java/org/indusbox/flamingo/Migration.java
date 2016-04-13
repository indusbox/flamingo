package org.indusbox.flamingo;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

public class Migration {

  public static void main(String[] args) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException, ParseException {
    Properties config = Configuration.load();
    ElasticsearchClient esClient = new ElasticsearchClient(config);

    String esIndexName = config.getProperty("elasticsearch.index");
    String esTypeName = "migration-metadata";

    String scriptsDir = config.getProperty("flamingo.scriptsDir");
    List<File> scripts = ScriptFile.getScripts(scriptsDir);
    if (scripts.isEmpty()) {
      System.out.println("No script, no migration");
      System.exit(0);
    } else {
      if (!esClient.indexExists()) {
        System.out.println("Index doesn't exist, creating index");
        if (!esClient.createIndex()) {
          throw new RuntimeException("Error while creating index " + esIndexName);
        }
      }
      if (!esClient.typeExists()) {
        if (!esClient.createType()) {
          throw new RuntimeException("Error while creating type " + esTypeName + " in index " + esIndexName);
        }
      }
      migrate(scripts, esClient);
    }
  }

  @SuppressWarnings("unchecked")
  public static void migrate(List<File> scripts, ElasticsearchClient esClient) throws IOException, ParseException {

    JSONObject latestSuccessfulScript = esClient.getLatestSuccessfulScript();
    System.out.println("latestSuccessfulScript " + latestSuccessfulScript);
    Long count = esClient.count();
    if (count == 0) {
      // No script were executed, starting migration from the beginning
      for (File script : scripts) {
        esClient.executeScript(script);
      }
    } else {
      JSONArray scriptObjects = esClient.list(count);
      // Check consistency (exists + checksum)
      for (Object scriptObject : scriptObjects) {
        JSONObject scriptJson = (JSONObject) scriptObject;
        final String fileName = (String) ((JSONObject) scriptJson.get("_source")).get("fileName");
        final String checksum = (String) ((JSONObject) scriptJson.get("_source")).get("checksum");
        Optional<File> scriptFound = Iterables.tryFind(scripts, new Predicate<File>() {
          @Override
          public boolean apply(File input) {
            return fileName.equals(input.getName());
          }
        });
        if (scriptFound.isPresent()) {
          String currentChecksum = Files.hash(scriptFound.get(), Hashing.md5()).toString();
          if (!Objects.equals(checksum, currentChecksum)) {
            throw new IllegalStateException("Abort migration. Checksum is different for script " + fileName + "!");
          }
        } else {
          throw new IllegalStateException("Abort migration. Script " + fileName + " doesn't exist anymore!");
        }
      }
      // No more than one fail script
      JSONArray failScripts = esClient.getFailScripts();
      if (failScripts.size() > 1) {
        throw new IllegalStateException("Abort migration. More than one failed script!");
      }
      if (!failScripts.isEmpty()) {
        JSONObject failScriptJson = (JSONObject) failScripts.get(0);
        JSONObject latestScript = esClient.getLatestScript();
        String id = (String) failScriptJson.get("_id");
        if (!Objects.equals(id, latestScript.get("_id"))) {
          throw new IllegalArgumentException("Abort migration. Fail script must be the latest script!");
        }
        final String fileName = (String) ((JSONObject) failScriptJson.get("_source")).get("fileName");
        // Retry failed script
        System.out.println("Retrying failed script " + fileName);
        File script = Iterables.find(scripts, new Predicate<File>() {
          @Override
          public boolean apply(File input) {
            return fileName.equals(input.getName());
          }
        });
        esClient.updateScript(script, id);
      }
      for (File script : scripts) {
        if (esClient.searchScript(script.getName()) == null) {
          esClient.executeScript(script);
        }
      }
    }
  }
}
