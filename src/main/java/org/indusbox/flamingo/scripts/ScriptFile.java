package org.indusbox.flamingo.scripts;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.io.Files;

public final class ScriptFile {

  private ScriptFile() {
  }

  public static List<File> getScripts(File scriptsDir) throws IOException {
    if (!scriptsDir.exists()) {
      throw new RuntimeException("Scripts directory " + scriptsDir + " doesn't exists");
    }
    final File[] templatesArray = listScripts(scriptsDir);

    if (templatesArray == null || templatesArray.length == 0) {
      return new ArrayList<>();
    }

    List<File> result = new ArrayList<>();

    for (File file : templatesArray) {
      if (file.isDirectory()) {
        // add dir content
        final File[] subDirFiles = listScripts(file);
        result.addAll(Arrays.asList(subDirFiles));
      } else {
        // add file
        result.add(file);
      }
    }

    Collections.sort(result, new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        return o1.getPath().compareTo(o2.getPath());
      }
    });

    return result;
  }

  /**
   * Lists json files and directories, all beginning with a number then a '_'.
   *
   * @param baseDir
   *         : Base directory for search. Must be not null.
   * @return A file array, or null if nothing found.
   */
  private static File[] listScripts(File baseDir) {

    return baseDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        boolean hasJsonFileExtension = "json".equals(Files.getFileExtension(name));
        if (!(hasJsonFileExtension || new File(dir, name).isDirectory())) {
          System.err.println("Ignoring script " + name + ". File name must be have .json extension.");
          return false;
        }
        Integer index = getIndex(name);
        if (index == null) {
          System.err.println("Ignoring script " + name + ". File name must start with an index number followed by an underscore and a description.");
          return false;
        }
        return true;
      }
    });
  }

  public static Integer getIndex(String scriptName) {

    String nameWithoutExtension = Files.getNameWithoutExtension(scriptName);
    final Iterator<String> stringIterator = Splitter.on('_').split(nameWithoutExtension).iterator();
    if (stringIterator.hasNext()) {
      String strIndex = stringIterator.next();
      try {
        return Integer.parseInt(strIndex);
      } catch (NumberFormatException e) {
        return null;
      }

    }
    return null;
  }

  public static List<File> getScripts(String scriptsDir) throws IOException {
    return getScripts(new File(scriptsDir));
  }
}
