package org.indusbox.flamingo;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

public class Migration {

  public static void main(String[] args) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {
    Properties config = ConfigurationLoader.load();
    FlamingoSettings settings = FlamingoSettings.fromConfig(config);
    migrate(settings);
  }

  public static int migrate(FlamingoSettings settings) throws IOException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
    ScriptManager scriptManager = new ScriptManager(settings);
    List<File> scripts = ScriptFile.getScripts(settings.getScriptsDir());
    if (scripts.isEmpty()) {
      System.out.println("No script, no migration");
      System.exit(0);
      return 0;
    } else {
      if (!scriptManager.indexFlamingoExists()) {
        System.out.println("Index doesn't exist, creating index");
        if (!scriptManager.createFlamingoIndex()) {
          throw new RuntimeException("Error while creating index " + settings.getIndexName());
        }
      }
      Long count = scriptManager.count();
      if (count == 0) {
        // No script were executed, starting migration from the beginning
        for (File script : scripts) {
          scriptManager.executeScript(script);
        }
        return scripts.size();
      }
      // No more than one fail script
      List<ScriptMetadata> failScripts = scriptManager.getFailScripts();
      if (failScripts.size() > 1) {
        throw new IllegalStateException("Abort migration. More than one failed script!");
      }
      // Check consistency (exists + checksum)
      List<ScriptMetadata> scriptsMetadata = scriptManager.list(count);
      for (ScriptMetadata scriptMetadata : scriptsMetadata) {
        final String fileName = scriptMetadata.getFileName();
        final String checksum = scriptMetadata.getChecksum();
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
          scripts.remove(scriptFound.get());
        } else {
          throw new IllegalStateException("Abort migration. Script " + fileName + " doesn't exist anymore!");
        }
      }
      if (!failScripts.isEmpty()) {
        ScriptMetadata failScript = failScripts.get(0);
        ScriptMetadata latestScript = scriptManager.getLatestScript();
        String id = failScript.getId();
        if (!Objects.equals(id, latestScript.getId())) {
          throw new IllegalArgumentException("Abort migration. Fail script must be the latest script!");
        }
        final String fileName = failScript.getFileName();
        // Retry failed script
        System.out.println("Retrying failed script " + fileName);
        File script = Iterables.find(scripts, new Predicate<File>() {
          @Override
          public boolean apply(File input) {
            return fileName.equals(input.getName());
          }
        });
        scriptManager.updateScript(script, id);
      }
      if (scripts.isEmpty()) {
        System.out.println("No new script to apply");
        return 0;
      }
      int result = 0;
      for (File script : scripts) {
        if (!scriptManager.scriptExists(script.getName())) {
          scriptManager.executeScript(script);
          result++;
        }
      }
      return result;
    }
  }
}
