package com.doofus.marketdata.downloader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.zip.ZipInputStream;

public class DownloaderUtils {

  static final DateTimeFormatter FILE_DATE_FORMAT = DateTimeFormatter.ofPattern("ddMMyy");
  static final DateTimeFormatter OBJECT_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/dd");

  static String getFileDateFormat(LocalDate isoDate) {
    return isoDate.format(FILE_DATE_FORMAT);
  }

  static String getObjectDateFormat(LocalDate isoDate) {
    return isoDate.format(OBJECT_DATE_FORMAT);
  }

  public static byte[] readEntryIntoInputStream(ZipInputStream zipInputStream) {
    byte[] buffer = new byte[2048];
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    int len;
    try {
      while (((len = zipInputStream.read(buffer)) > 0)) {
        outputStream.write(buffer, 0, len);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return outputStream.toByteArray();
  }
}
