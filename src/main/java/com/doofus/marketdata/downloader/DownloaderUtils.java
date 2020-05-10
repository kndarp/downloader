package com.doofus.marketdata.downloader;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class DownloaderUtils {

  static final DateTimeFormatter FILE_DATE_FORMAT = DateTimeFormatter.ofPattern("ddMMyy");
  static final DateTimeFormatter OBJECT_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/dd");

  static String getFileDateFormat(LocalDate isoDate) {
    return isoDate.format(FILE_DATE_FORMAT);
  }

  static String getObjectDateFormat(LocalDate isoDate) {
    return isoDate.format(OBJECT_DATE_FORMAT);
  }

  static List<String> unzip(String filePath) throws IOException {
    final List<String> extractedFilePaths = new ArrayList<>();
    final String directory = "extracts/";
    new File(directory).mkdir();
    final ZipFile zipFile = new ZipFile(filePath);
    final Enumeration<? extends ZipEntry> entries = zipFile.entries();

    while (entries.hasMoreElements()) {
      ZipEntry zipEntry = entries.nextElement();
      String destPath = directory + zipEntry.getName();

      if (!zipEntry.isDirectory()
          && FilenameUtils.getExtension(zipEntry.toString()).equalsIgnoreCase("csv")) {
        try (InputStream inputStream = zipFile.getInputStream(zipEntry);
            FileOutputStream outputStream = new FileOutputStream(destPath)) {
          int data = inputStream.read();
          while (data != -1) {
            outputStream.write(data);
            data = inputStream.read();
          }
        }
        extractedFilePaths.add(destPath);
      }
    }
    return extractedFilePaths;
  }

  static void delete(String filePath) {
    FileUtils.deleteQuietly(new File(filePath));
  }
}
