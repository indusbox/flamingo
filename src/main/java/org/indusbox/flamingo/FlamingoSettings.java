package org.indusbox.flamingo;

import java.io.File;
import java.util.Properties;

import com.google.common.base.Strings;

public class FlamingoSettings {

  private File scriptsDir;

  private String indexName = ".flamingo";

  private String protocol;
  private String hostName;
  private int port;

  private String username;
  private String password;

  public static FlamingoSettings fromConfig(Properties config) {
    FlamingoSettings flamingoSettings = new FlamingoSettings()
        .setScriptsDir(new File(config.getProperty("flamingo.scriptsDir")))
        .setUsername(config.getProperty("elasticsearch.user"))
        .setPassword(config.getProperty("elasticsearch.password"))
        .setProtocol(config.getProperty("elasticsearch.protocol"))
        .setHostName(config.getProperty("elasticsearch.host"))
        .setPort(Integer.valueOf(config.getProperty("elasticsearch.port")));
    String indexNameValue = config.getProperty("elasticsearch.index");
    if (!Strings.isNullOrEmpty(indexNameValue)) {
      flamingoSettings.setIndexName(indexNameValue);
    }
    return flamingoSettings;
  }

  public File getScriptsDir() {
    return scriptsDir;
  }

  public FlamingoSettings setScriptsDir(File scriptsDir) {
    this.scriptsDir = scriptsDir;
    return this;
  }

  public String getIndexName() {
    return indexName;
  }

  public FlamingoSettings setIndexName(String indexName) {
    this.indexName = indexName;
    return this;
  }

  public String getProtocol() {
    return protocol;
  }

  public FlamingoSettings setProtocol(String protocol) {
    this.protocol = protocol;
    return this;
  }

  public String getHostName() {
    return hostName;
  }

  public FlamingoSettings setHostName(String hostName) {
    this.hostName = hostName;
    return this;
  }

  public int getPort() {
    return port;
  }

  public FlamingoSettings setPort(int port) {
    this.port = port;
    return this;
  }

  public String getUsername() {
    return username;
  }

  public FlamingoSettings setUsername(String username) {
    this.username = username;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public FlamingoSettings setPassword(String password) {
    this.password = password;
    return this;
  }
}
