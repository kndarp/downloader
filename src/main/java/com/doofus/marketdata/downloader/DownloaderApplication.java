package com.doofus.marketdata.downloader;

import org.apache.commons.io.FilenameUtils;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.Instant;
import java.time.LocalDate;

import static com.doofus.marketdata.downloader.GoogleCloudStorageHelper.uploadObject;

@SpringBootApplication
@RestController
public class DownloaderApplication {
  private static final Logger LOGGER = LoggerFactory.getLogger(DownloaderApplication.class);

  final String downloadLink = "https://www.bseindia.com/download/BhavCopy/Equity/EQ%s_CSV.ZIP";

  @Autowired private Environment environment;

  public static void main(String[] args) {
    SpringApplication.run(DownloaderApplication.class, args);
  }

  @GetMapping("/download/bse")
  public ResponseEntity<String> downloadBse(
      @RequestParam(value = "date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {

    String objectPath = String.format("bse/%s/", DownloaderUtils.getObjectDateFormat(date));
    String downloadedFile = String.valueOf(Instant.now().getEpochSecond());

    try (FileOutputStream fos = new FileOutputStream(downloadedFile)) {

      URL website = new URL(String.format(downloadLink, DownloaderUtils.getFileDateFormat(date)));
      ReadableByteChannel rbc = Channels.newChannel(website.openStream());
      fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

      DownloaderUtils.unzip(downloadedFile)
          .forEach(
              filePath -> {
                try {
                  uploadObject(
                      environment.getProperty("PROJECT"),
                      environment.getProperty("BUCKET"),
                      objectPath + FilenameUtils.getName(filePath),
                      filePath);
                  DownloaderUtils.delete(filePath);
                } catch (IOException e) {
                  e.printStackTrace();
                }
              });
    } catch (FileNotFoundException e) {
      LOGGER.warn("No file for date {}", date, e);
      return ResponseEntity.ok("No File for date");
    } catch (IOException e) {
      LOGGER.error("Problems with IO", e);
      ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.fillInStackTrace());
    } finally {
      DownloaderUtils.delete(downloadedFile);
    }
    return ResponseEntity.ok("Download Succesful");
    // TODO redirect to conversion service
  }
}
