package org.indusbox.flamingo;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ElasticsearchNode {

  private Client client;

  public ElasticsearchNode() {
    NodeBuilder nodeBuilder = new NodeBuilder();
    nodeBuilder.settings(ImmutableSettings.settingsBuilder()
        .put("path.logs", "target/es/log")
        .put("path.data", "target/es/data")
        .put("cluster.name", "test-cluster")
        .put("node.name", "test-node")
        .build());
    nodeBuilder.local(true);
    Node node = nodeBuilder.node();
    this.client = node.client();
  }

  public Client getClient() {
    return client;
  }
}
