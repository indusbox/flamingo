package org.indusbox.flamingo.scripts;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.json.simple.JSONObject;

public class ScriptMetadata {

  private String id;
  private String fileName;
  private String checksum;
  private DateTime executedDate;
  private boolean succeeded;

  public ScriptMetadata(String id, String fileName, String checksum, DateTime executedDate, boolean succeeded) {
    this.id = id;
    this.fileName = fileName;
    this.checksum = checksum;
    this.executedDate = executedDate;
    this.succeeded = succeeded;
  }

  public static ScriptMetadata fromJSON(JSONObject scriptJSON) {
    return new ScriptMetadata(
        (String) scriptJSON.get("_id"),
        (String) ((JSONObject) scriptJSON.get("_source")).get("fileName"),
        (String) ((JSONObject) scriptJSON.get("_source")).get("checksum"),
        DateTime.parse((String) ((JSONObject) scriptJSON.get("_source")).get("executedDate"), ISODateTimeFormat.dateHourMinuteSecondMillis()),
        (Boolean) ((JSONObject) scriptJSON.get("_source")).get("succeeded")
    );
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getChecksum() {
    return checksum;
  }

  public void setChecksum(String checksum) {
    this.checksum = checksum;
  }

  public DateTime getExecutedDate() {
    return executedDate;
  }

  public void setExecutedDate(DateTime executedDate) {
    this.executedDate = executedDate;
  }

  public boolean isSucceeded() {
    return succeeded;
  }

  public void setSucceeded(boolean succeeded) {
    this.succeeded = succeeded;
  }
}
