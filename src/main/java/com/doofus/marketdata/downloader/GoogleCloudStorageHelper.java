package com.doofus.marketdata.downloader;

import com.google.cloud.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GoogleCloudStorageHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudStorageHelper.class);

  public static void uploadObject(
      String projectId, String bucketName, String objectName, String filePath) throws IOException {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    BlobId blobId = BlobId.of(bucketName, objectName);
    Blob blob = storage.get(blobId);

    if (blob == null || !blob.exists()) {
      LOGGER.warn("File already exists. Not Uploading it");
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
      storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
    }
  }
}
