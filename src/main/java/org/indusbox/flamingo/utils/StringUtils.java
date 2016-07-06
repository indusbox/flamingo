package org.indusbox.flamingo.utils;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Utility class to manipulate Strings, allowing not to import bigger third party lib.
 * Credits to Apache Commons Lang 3 => https://github.com/apache/commons-lang/
 */
public class StringUtils {

  private StringUtils() {
    // default constructor, no instantiation.
  }

  /**
   * <p>Removes a substring only if it is at the begining of a source string,
   * otherwise returns the source string.</p>
   * <p/>
   * <p>A <code>null</code> source string will return <code>null</code>.
   * An empty ("") source string will return the empty string.
   * A <code>null</code> search string will return the source string.</p>
   * <p/>
   * <pre>
   * StringUtils.removeStart(null, *)      = null
   * StringUtils.removeStart("", *)        = ""
   * StringUtils.removeStart(*, null)      = *
   * StringUtils.removeStart("www.domain.com", "www.")   = "domain.com"
   * StringUtils.removeStart("domain.com", "www.")       = "domain.com"
   * StringUtils.removeStart("www.domain.com", "domain") = "www.domain.com"
   * StringUtils.removeStart("abc", "")    = "abc"
   * </pre>
   *
   * @param str
   *         the source String to search, may be null
   * @param remove
   *         the String to search for and remove, may be null
   * @return the substring with the string removed if found,
   * <code>null</code> if null String input
   * @since 2.1
   */
  public static String removeStart(String str, String remove) {
    if (isNullOrEmpty(str) || isNullOrEmpty(remove)) {
      return str;
    }
    if (str.startsWith(remove)) {
      return str.substring(remove.length());
    }
    return str;
  }

}
