package org.indusbox.flamingo.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * Based on https://github.com/apache/commons-lang/blob/master/src/test/java/org/apache/commons/lang3/StringUtilsTest.java.
 */
public class StringUtilsTest {

  @Test
  public void testRemoveStart() {
    // StringUtils.removeStart("", *)        = ""
    assertNull(StringUtils.removeStart(null, null));
    assertNull(StringUtils.removeStart(null, ""));
    assertNull(StringUtils.removeStart(null, "a"));

    // StringUtils.removeStart(*, null)      = *
    assertEquals("", StringUtils.removeStart("", null));
    assertEquals("", StringUtils.removeStart("", ""));
    assertEquals("", StringUtils.removeStart("", "a"));

    // All others:
    assertEquals("domain.com",StringUtils.removeStart("www.domain.com", "www."));
    assertEquals("domain.com", StringUtils.removeStart("domain.com", "www."));
    assertEquals("domain.com", StringUtils.removeStart("domain.com", ""));
    assertEquals("domain.com", StringUtils.removeStart("domain.com", null));
  }


}
