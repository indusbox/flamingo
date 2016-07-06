package org.indusbox.flamingo;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.indusbox.flamingo.scripts.ScriptFile;
import org.indusbox.flamingo.scripts.ScriptManager;
import org.indusbox.flamingo.scripts.ScriptMetadata;
import org.indusbox.flamingo.settings.ConfigurationLoader;
import org.indusbox.flamingo.settings.FlamingoSettings;

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
    final File scriptsDir = settings.getScriptsDir();
    List<File> scripts = ScriptFile.getScripts(scriptsDir);
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
          scriptManager.executeScript(script, scriptsDir);
        }
        return scripts.size();
      }

      List<ScriptMetadata> failScripts = handleFailedScripts(scriptManager, scriptsDir, scripts);

      // Check consistency (exists + checksum)
      List<ScriptMetadata> scriptsMetadata = scriptManager.list(count);
      List<File> remainingScript = new ArrayList<>(scripts);
      int index = 0;
      for (ScriptMetadata scriptMetadata : scriptsMetadata) {
        final String scriptMetadataFilename = buildFilename(scriptsDir, scriptMetadata.getFileName());
        final String checksum = scriptMetadata.getChecksum();
        Optional<File> scriptFound = lookupScript(scriptMetadataFilename, scripts);
        if (scriptFound.isPresent()) {
          String currentChecksum = Files.hash(scriptFound.get(), Hashing.md5()).toString();
          if (!Objects.equals(checksum, currentChecksum) && !failScripts.contains(scriptMetadata)) {
            throw new IllegalStateException("Abort migration. Checksum is different for script " + scriptMetadataFilename + "!");
          }
          final File script = scripts.get(index);

          if (!script.getCanonicalPath().equals(scriptMetadataFilename)) {
            throw new IllegalStateException("A new script has been inserted before last successfully executed script (" + script.getCanonicalPath() + ")!");
          }
          remainingScript.remove(scriptFound.get());
        } else {
          throw new IllegalStateException("Abort migration. Script " + scriptMetadataFilename + " doesn't exist anymore!");
        }
        index++;
      }

      // everything's fine, let's update !
      return processNewScripts(scriptManager, scriptsDir, remainingScript);
    }
  }

  private static Integer getIndex(final String filename, List<ScriptMetadata> scriptMetadatas, final File scriptDir) {
    return Iterables.indexOf(scriptMetadatas, new Predicate<ScriptMetadata>() {
      @Override
      public boolean apply(ScriptMetadata input) {
        return filename.equals(buildFilename(scriptDir, input.getFileName()));
      }
    });
  }

  private static int processNewScripts(ScriptManager scriptManager, File scriptsDir, List<File> scripts) throws IOException {
    if (scripts.isEmpty()) {
      System.out.println("No new script to apply");
      return 0;
    }
    // Process remaining scripts
    int result = 0;
    for (File script : scripts) {
      if (!scriptManager.scriptExists(script.getName())) {
        scriptManager.executeScript(script, scriptsDir);
        result++;
      }
    }
    return result;
  }

  private static List<ScriptMetadata> handleFailedScripts(ScriptManager scriptManager, File scriptsDir, List<File> scripts) throws IOException {
    // No more than one fail script
    List<ScriptMetadata> failScripts = scriptManager.getFailScripts();
    if (failScripts.size() > 1) {
      throw new IllegalStateException("Abort migration. More than one failed script!");
    }
    // handle failed scripts
    if (!failScripts.isEmpty()) {
      ScriptMetadata failScript = failScripts.get(0);
      ScriptMetadata latestScript = scriptManager.getLatestScript();
      String id = failScript.getId();
      if (!Objects.equals(id, latestScript.getId())) {
        throw new IllegalStateException("Abort migration. Fail script must be the latest script!");
      }
      final String fileName = buildFilename(scriptsDir, failScript.getFileName());
      // Retry failed script
      System.out.println("Retrying failed script " + fileName);
      File script = lookupScript(fileName, scripts).get();
      scriptManager.updateScript(script, scriptsDir, id);
    }
    return failScripts;
  }

  private static Optional<File> lookupScript(final String filename, final List<File> scripts) {
    return Iterables.tryFind(scripts, new Predicate<File>() {
      @Override
      public boolean apply(File input) {
        try {
          return filename.equals(input.getCanonicalPath());
        } catch (IOException e) {
          System.err.println("Error during filename comparison :");
          e.printStackTrace();
          return false;
        }
      }
    });
  }

  private static String buildFilename(File scriptsDir, String scriptName) {
    return scriptsDir + File.separator + scriptName.replace("/", File.separator);
  }
}
