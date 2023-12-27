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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.local.LocalObjectIO;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class S3ObjectIO implements ObjectIO {

  private static final ObjectIO local = new LocalObjectIO();

  private final S3Client s3client =
      S3Client.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build();

  @Override
  public InputStream readObject(URI uri) throws IOException {
    if (!"s3".equals(uri.getScheme())) {
      return local.readObject(uri);
    }

    S3Uri s3uri = s3client.utilities().parseUri(uri);

    return s3client.getObject(
        GetObjectRequest.builder()
            .bucket(s3uri.bucket().orElseThrow())
            .key(s3uri.key().orElseThrow())
            .build());
  }
}
