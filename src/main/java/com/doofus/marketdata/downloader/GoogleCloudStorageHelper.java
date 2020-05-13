package com.doofus.marketdata.downloader;

import com.google.cloud.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleCloudStorageHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudStorageHelper.class);

  public static void uploadObject(
      String projectId, String bucketName, String objectName, byte[] inputStream) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    BlobId blobId = BlobId.of(bucketName, objectName);
    Blob blob = storage.get(blobId);

    if (blob == null || !blob.exists()) {
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
      storage.create(blobInfo, inputStream);
      LOGGER.info("{} Uploaded", objectName);
    } else {
      LOGGER.warn("File already exists. Not Uploading it");
    }
  }
}
