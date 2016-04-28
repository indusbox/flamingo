package org.indusbox.flamingo.elasticsearch;

import java.util.Properties;

public class ElasticsearchSettings {

  private String indexName;

  private String protocol;
  private String hostName;
  private int port;

  private String username;
  private String password;

  public static ElasticsearchSettings fromConfig(Properties config) {
    return new ElasticsearchSettings()
        .setUsername(config.getProperty("elasticsearch.user"))
        .setPassword(config.getProperty("elasticsearch.password"))
        .setProtocol(config.getProperty("elasticsearch.protocol"))
        .setHostName(config.getProperty("elasticsearch.host"))
        .setPort(Integer.valueOf(config.getProperty("elasticsearch.port")))
        .setIndexName(config.getProperty("elasticsearch.index"));
  }

  public String getIndexName() {
    return indexName;
  }

  public ElasticsearchSettings setIndexName(String indexName) {
    this.indexName = indexName;
    return this;
  }

  public String getProtocol() {
    return protocol;
  }

  public ElasticsearchSettings setProtocol(String protocol) {
    this.protocol = protocol;
    return this;
  }

  public String getHostName() {
    return hostName;
  }

  public ElasticsearchSettings setHostName(String hostName) {
    this.hostName = hostName;
    return this;
  }

  public int getPort() {
    return port;
  }

  public ElasticsearchSettings setPort(int port) {
    this.port = port;
    return this;
  }

  public String getUsername() {
    return username;
  }

  public ElasticsearchSettings setUsername(String username) {
    this.username = username;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public ElasticsearchSettings setPassword(String password) {
    this.password = password;
    return this;
  }
}
