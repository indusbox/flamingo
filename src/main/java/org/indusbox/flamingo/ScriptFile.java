package org.indusbox.flamingo;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;

public final class ScriptFile {

  private ScriptFile() {
  }

  public static List<File> getScripts(File scriptsDir) throws IOException {
    if (!scriptsDir.exists()) {
      throw new RuntimeException("Scripts directory " + scriptsDir + " doesn't exists");
    }
    File[] templatesArray = scriptsDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        boolean hasJsonFileExtension = "json".equals(Files.getFileExtension(name));
        if (!hasJsonFileExtension) {
          System.err.println("Ignoring script " + name + ". File name must be have .json extension.");
          return false;
        }
        Integer index = getIndex(name);
        if (index == null) {
          System.err.println("Ignoring script " + name + ". File name must be a numeric.");
          return false;
        }
        return true;
      }
    });
    if (templatesArray == null || templatesArray.length == 0) {
      return new ArrayList<>();
    }
    List<File> scripts = Lists.newArrayList(templatesArray);
    Collections.sort(scripts, Ordering.from(new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        Integer i1 = Integer.parseInt(Files.getNameWithoutExtension(o1.getName()));
        Integer i2 = Integer.parseInt(Files.getNameWithoutExtension(o2.getName()));
        return i1.compareTo(i2);
      }
    }));
    return scripts;
  }

  public static Integer getIndex(String scriptName) {
    String nameWithoutExtension = Files.getNameWithoutExtension(scriptName);
    try {
      return Integer.parseInt(nameWithoutExtension);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static List<File> getScripts(String scriptsDir) throws IOException {
    return getScripts(new File(scriptsDir));
  }
}
