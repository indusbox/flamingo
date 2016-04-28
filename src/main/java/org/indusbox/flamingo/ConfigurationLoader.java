package org.indusbox.flamingo;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Strings;

public final class ConfigurationLoader {

  private ConfigurationLoader() {
  }

  public static Properties load() throws IOException {
    String configFile = System.getProperty("configFile");
    if (Strings.isNullOrEmpty(configFile)) {
      throw new IllegalArgumentException("Use the system property -DconfigFile to configure Flamingo");
    }
    final FileInputStream in = new FileInputStream(configFile);
    Properties config = new Properties();
    config.load(in);
    return config;
  }
}
