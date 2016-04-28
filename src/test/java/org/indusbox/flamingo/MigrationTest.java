package org.indusbox.flamingo;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;
import static org.assertj.core.api.Java6Assertions.assertThat;

import java.io.File;
import java.net.URISyntaxException;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.indusbox.flamingo.elasticsearch.ElasticsearchSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.tlrx.elasticsearch.test.EsSetup;

public class MigrationTest {

  private Client client = new ElasticsearchNode().getClient();

  @Before
  public void setUp() {
    new EsSetup(client).execute(createIndex("library"));
  }

  @Test
  public void should_throw_an_exception_if_migration_script_is_empty() throws Exception {
    MigrationSettings migrationSettings = createMigrationSettings("library_1");
    try {
      Migration.migrate(migrationSettings);
    } catch (RuntimeException e) {
      assertThat(e).hasMessageContaining("Error while executing 1.json");
    }
  }

  @Test
  public void should_apply_migration() throws Exception {
    MigrationSettings migrationSettings = createMigrationSettings("library_2");
    Migration.migrate(migrationSettings);
    assertThat(new EsSetup(client).exists("library", "book", "1")).isTrue();
  }

  private MigrationSettings createMigrationSettings(String scriptDir) throws URISyntaxException {
    NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().setHttp(true).get();
    InetSocketTransportAddress publishAddress = (InetSocketTransportAddress) nodeInfos.getNodes()[0].getHttp().address().publishAddress();
    String hostName = publishAddress.address().getHostName();
    int port = publishAddress.address().getPort();
    ElasticsearchSettings elasticsearchSettings = new ElasticsearchSettings()
        .setHostName(hostName)
        .setPort(port)
        .setProtocol("http")
        .setIndexName("library");
    return new MigrationSettings()
        .setElasticsearchSettings(elasticsearchSettings)
        .setScriptsDir(new File(MigrationTest.class.getClassLoader().getResource("./" + scriptDir).toURI()));
  }

  @After
  public void tearDown() {
    new EsSetup(client).execute(deleteAll());
  }

}
