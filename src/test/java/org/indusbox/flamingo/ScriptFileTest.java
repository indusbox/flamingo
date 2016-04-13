package org.indusbox.flamingo;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.junit.Test;

public class ScriptFileTest {

  @Test
  public void should_ignore_invalid_file_name() throws IOException, URISyntaxException {
    URL resource = ScriptFileTest.class.getClassLoader().getResource("./scripts");
    if (resource == null) {
      throw new RuntimeException("Unable to find scripts directory");
    }
    List<File> scripts = ScriptFile.getScripts(new File(resource.toURI()));
    assertThat(scripts).extracting("name").containsExactly("1.json", "2.json", "10.json");
  }

  @Test
  public void should_throws_an_exception_if_scripts_directory_doesnt_exists() throws IOException, URISyntaxException {
    try {
      ScriptFile.getScripts("unknown");
    } catch (RuntimeException e) {
      assertThat(e).hasMessage("Scripts directory unknown doesn't exists");
    }
  }
}
