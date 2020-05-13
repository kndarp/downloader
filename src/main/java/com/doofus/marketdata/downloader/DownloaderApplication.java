package com.doofus.marketdata.downloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.doofus.marketdata.downloader.GoogleCloudStorageHelper.uploadObject;

@SpringBootApplication
@RestController
@RequestMapping("/download")
public class DownloaderApplication {
  private static final Logger LOGGER = LoggerFactory.getLogger(DownloaderApplication.class);

  final String downloadLink = "https://www.bseindia.com/download/BhavCopy/Equity/EQ%s_CSV.ZIP";

  @Autowired private Environment environment;

  public static void main(String[] args) {
    SpringApplication.run(DownloaderApplication.class, args);
  }

  @GetMapping("/bse")
  public ResponseEntity<String> downloadBse(
      @RequestParam(value = "date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {

    String objectPath = String.format("bse/%s/", DownloaderUtils.getObjectDateFormat(date));

    try {
      URL downloadUrl =
          new URL(String.format(downloadLink, DownloaderUtils.getFileDateFormat(date)));
      ZipInputStream zipInputStream = new ZipInputStream(downloadUrl.openStream());

      ZipEntry zipEntry;
      while ((zipEntry = zipInputStream.getNextEntry()) != null) {

        byte[] zipEntryBytes = DownloaderUtils.readEntryIntoInputStream(zipInputStream);
        uploadObject(
            environment.getProperty("PROJECT"),
            environment.getProperty("BUCKET"),
            objectPath + zipEntry.getName(),
            zipEntryBytes);
      }

    } catch (MalformedURLException ex) {
      LOGGER.error("Check URL formation", ex);
    } catch (FileNotFoundException e) {
      LOGGER.warn("No file for date {}", date, e);
      return ResponseEntity.ok("No File for date");
    } catch (IOException e) {
      LOGGER.error("Problems with IO", e);
      ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.fillInStackTrace());
    }
    return ResponseEntity.ok("Download Succesful");
    // TODO redirect to conversion service
  }
}
