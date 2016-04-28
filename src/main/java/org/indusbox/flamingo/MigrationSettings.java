package org.indusbox.flamingo;

import java.io.File;
import java.util.Properties;

import org.indusbox.flamingo.elasticsearch.ElasticsearchSettings;

import com.google.common.base.Strings;

public class MigrationSettings {

  private ElasticsearchSettings elasticsearchSettings;
  private File scriptsDir;
  private String typeName = "migration-metadata";

  public static MigrationSettings fromConfig(Properties config) {
    MigrationSettings migrationSettings = new MigrationSettings();
    migrationSettings
        .setElasticsearchSettings(ElasticsearchSettings.fromConfig(config))
        .setScriptsDir(new File(config.getProperty("flamingo.scriptsDir")));
    String typeName = config.getProperty("flamingo.typeName");
    if (!Strings.isNullOrEmpty(typeName)) {
      migrationSettings.setTypeName(typeName);
    }
    return migrationSettings;
  }

  public ElasticsearchSettings getElasticsearchSettings() {
    return elasticsearchSettings;
  }

  public MigrationSettings setElasticsearchSettings(ElasticsearchSettings elasticsearchSettings) {
    this.elasticsearchSettings = elasticsearchSettings;
    return this;
  }

  public File getScriptsDir() {
    return scriptsDir;
  }

  public MigrationSettings setScriptsDir(File scriptsDir) {
    this.scriptsDir = scriptsDir;
    return this;
  }

  public String getTypeName() {
    return typeName;
  }

  public MigrationSettings setTypeName(String typeName) {
    this.typeName = typeName;
    return this;
  }
}
