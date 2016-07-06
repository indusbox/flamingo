package org.indusbox.flamingo;

import static org.assertj.core.api.Java6Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.indusbox.flamingo.settings.FlamingoSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MigrationTest {

  private ElasticsearchLocalNode localNode;
  private Client client;

  @Before
  public void setUp() throws IOException {
    localNode = new ElasticsearchLocalNode();
    this.client = localNode.getClient();
    client.admin().indices().prepareCreate("library").get();
  }

  @Test
  public void should_apply_migration() throws Exception {
    FlamingoSettings settings = createFlamingoSettings("library_2");
    Migration.migrate(settings);
    assertThat(client.prepareGet("library", "book", "1").get().isExists()).isTrue();
  }

  @Test
  public void should_not_apply_migration_twice() throws Exception {
    FlamingoSettings settings = createFlamingoSettings("library_2");
    int scriptsApplied = Migration.migrate(settings);
    assertThat(client.prepareGet("library", "book", "1").get().isExists()).isTrue();
    assertThat(scriptsApplied).isEqualTo(1);

    // Nothing new, should not apply any migration
    scriptsApplied = Migration.migrate(settings);
    assertThat(client.prepareGet("library", "book", "1").get().isExists()).isTrue();
    assertThat(scriptsApplied).isEqualTo(0);
  }

  @Test
  public void should_apply_multiple_migration_scripts_in_one_execution() throws Exception {
    FlamingoSettings settings = createFlamingoSettings("library_4");
    int scriptsApplied = Migration.migrate(settings);
    assertThat(client.prepareGet("library", "book", "1").get().isExists()).isTrue();
    assertThat(client.prepareGet("library", "book", "2").get().isExists()).isTrue();
    assertThat(scriptsApplied).isEqualTo(2);
  }

  @Test
  public void should_apply_one_script_then_another_one() throws Exception {
    FlamingoSettings settings = createFlamingoSettings("library_2");
    int scriptsApplied = Migration.migrate(settings);
    assertThat(client.prepareGet("library", "book", "1").get().isExists()).isTrue();
    assertThat(scriptsApplied).isEqualTo(1);

    // One new script!
    settings = createFlamingoSettings("library_4");
    scriptsApplied = Migration.migrate(settings);
    assertThat(client.prepareGet("library", "book", "2").get().isExists()).isTrue();
    assertThat(scriptsApplied).isEqualTo(1);
  }

  @Test
  public void should_throw_an_exception_if_a_script_checksum_is_different() throws Exception {
    FlamingoSettings settings = createFlamingoSettings("library_2");
    int scriptsApplied = Migration.migrate(settings);
    assertThat(client.prepareGet("library", "book", "1").get().isExists()).isTrue();
    assertThat(scriptsApplied).isEqualTo(1);

    // Script file 1.json has a different checksum
    settings = createFlamingoSettings("library_3");
    try {
      Migration.migrate(settings);
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageMatching(".*Checksum is different for script.*1\\.json!");
    }
  }

  @Test
  public void should_throw_an_exception_if_scripts_are_inconsistent() throws Exception {
    FlamingoSettings settings = createFlamingoSettings("library_2");
    int scriptsApplied = Migration.migrate(settings);
    assertThat(client.prepareGet("library", "book", "1").get().isExists()).isTrue();
    assertThat(scriptsApplied).isEqualTo(1);

    // Script file 1.json doesn't exist anymore!
    settings = createFlamingoSettings("library_5");
    try {
      Migration.migrate(settings);
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageContaining("1.json doesn't exist anymore");
    }
  }

  @Test
  public void should_throw_an_exception_if_flamingo_is_in_an_inconsistent_state() throws Exception {
    // TODO Create an inconsistent state then run Flamingo
  }

  @Test
  public void should_throw_an_exception_if_a_migration_script_is_invalid() throws Exception {
    FlamingoSettings settings = createFlamingoSettings("library_1");
    try {
      Migration.migrate(settings);
    } catch (RuntimeException e) {
      assertThat(e).hasMessageContaining("Error while executing 1.json");
    }
  }

  @Test
  public void should_throw_an_exception_if_a_new_migration_script_is_before_last_executed() throws Exception {
    // one script : n° 2
    FlamingoSettings settings = createFlamingoSettings("library_5");
    Migration.migrate(settings);

    // 2 scripts : n°1 and 2 => should raise an exception
    settings = createFlamingoSettings("library_4");
    try {
      Migration.migrate(settings);
    } catch (RuntimeException e) {
      assertThat(e).hasMessageContaining("A new script has been inserted before last successfully executed script");
    }
  }

  private FlamingoSettings createFlamingoSettings(String scriptDir) throws URISyntaxException {
    NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().setHttp(true).get();
    InetSocketTransportAddress publishAddress = (InetSocketTransportAddress) nodeInfos.getNodes()[0].getHttp().address().publishAddress();
    String hostName = publishAddress.address().getHostName();
    int port = publishAddress.address().getPort();
    return new FlamingoSettings()
        .setHostName(hostName)
        .setPort(port)
        .setProtocol("http")
        .setScriptsDir(new File(MigrationTest.class.getClassLoader().getResource("./" + scriptDir).toURI()));
  }

  @After
  public void tearDown() throws IOException {
    localNode.clean();
  }
}
