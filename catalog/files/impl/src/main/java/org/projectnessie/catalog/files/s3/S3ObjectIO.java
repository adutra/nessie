/*
 * Copyright (C) 2023 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.catalog.files.s3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.function.Supplier;
import org.projectnessie.catalog.files.api.BackendThrottledException;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.ObjectIOException;
import org.projectnessie.catalog.files.local.LocalObjectIO;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3ObjectIO implements ObjectIO {

  private static final ObjectIO local = new LocalObjectIO();

  private S3Client s3client;
  private final Supplier<S3Client> clientSupplier;
  private final Clock clock;
  private final Duration defaultRetryAfter;

  public S3ObjectIO(Clock clock, Duration defaultRetryAfter) {
    this(S3ObjectIO::buildS3Client, clock, defaultRetryAfter);
  }

  public S3ObjectIO(Supplier<S3Client> clientSupplier, Clock clock, Duration defaultRetryAfter) {
    this.clientSupplier = clientSupplier;
    this.clock = clock;
    this.defaultRetryAfter = defaultRetryAfter;
  }

  @Override
  public InputStream readObject(URI uri) throws IOException {
    if (!"s3".equals(uri.getScheme())) {
      return local.readObject(uri);
    }

    initClient();

    S3Uri s3uri = s3client.utilities().parseUri(uri);

    try {
      return s3client.getObject(
          GetObjectRequest.builder()
              .bucket(s3uri.bucket().orElseThrow())
              .key(s3uri.key().orElseThrow())
              .build());
    } catch (SdkServiceException e) {
      if (e.isThrottlingException()) {
        throw new BackendThrottledException(
            clock.instant().plus(defaultRetryAfter), "S3 throttled", e);
      }
      throw new ObjectIOException(e);
    }
  }

  @Override
  public OutputStream writeObject(URI uri) throws IOException {
    if (!"s3".equals(uri.getScheme())) {
      return local.writeObject(uri);
    }

    initClient();

    return new ByteArrayOutputStream() {
      @Override
      public void close() throws IOException {
        super.close();

        S3Uri s3uri = s3client.utilities().parseUri(uri);

        s3client.putObject(
            PutObjectRequest.builder()
                .bucket(s3uri.bucket().orElseThrow())
                .key(s3uri.key().orElseThrow())
                .build(),
            RequestBody.fromBytes(toByteArray()));
      }
    };
  }

  private void initClient() {
    if (s3client == null) {
      s3client = clientSupplier.get();
    }
  }

  private static S3Client buildS3Client() {
    return S3Client.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build();
  }
}
