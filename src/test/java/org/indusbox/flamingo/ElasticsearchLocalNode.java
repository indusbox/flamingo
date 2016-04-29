package org.indusbox.flamingo;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ElasticsearchLocalNode {

  private static final String ES_LOG = "es-log";
  private static final String ES_DATA = "es-data";
  private Path logDirectory;
  private Path dataDirectory;

  private Client client;
  private Node node;

  public ElasticsearchLocalNode() throws IOException {
    NodeBuilder nodeBuilder = new NodeBuilder();
    this.logDirectory = Files.createTempDirectory(ES_LOG);
    this.dataDirectory = Files.createTempDirectory(ES_DATA);
    nodeBuilder.settings(ImmutableSettings.settingsBuilder()
        .put("path.logs", this.logDirectory)
        .put("path.data", this.dataDirectory)
        .put("cluster.name", "test-cluster")
        .put("node.name", "test-node")
        .build());
    nodeBuilder.local(true);
    this.node = nodeBuilder.node();
    this.client = node.client();
  }

  public Client getClient() {
    return client;
  }

  public void clean() throws IOException {
    client.admin().indices().prepareDelete("_all").get();
    node.close();
    deleteRecursively(logDirectory);
    deleteRecursively(dataDirectory);
  }

  private void deleteRecursively(Path directory) throws IOException {
    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }
}
