/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.files;

import java.net.URI;
import java.time.Clock;
import org.projectnessie.catalog.files.adls.AdlsObjectIO;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.gcs.GcsObjectIO;
import org.projectnessie.catalog.files.local.LocalObjectIO;
import org.projectnessie.catalog.files.s3.S3Config;
import org.projectnessie.catalog.files.s3.S3ObjectIO;
import software.amazon.awssdk.services.s3.S3Client;

public class ResolvingObjectIO extends DelegatingObjectIO {
  private final LocalObjectIO localObjectIO;
  private final S3ObjectIO s3ObjectIO;
  private final GcsObjectIO gcsObjectIO;
  private final AdlsObjectIO adlsObjectIO;

  public ResolvingObjectIO(S3Client s3client, S3Config s3config) {
    localObjectIO = new LocalObjectIO();
    s3ObjectIO = new S3ObjectIO(s3client, Clock.systemUTC(), s3config);
    gcsObjectIO = new GcsObjectIO();
    adlsObjectIO = new AdlsObjectIO();
  }

  @Override
  protected ObjectIO resolve(URI uri) {
    String scheme = uri.getScheme();
    if ("s3".equals(scheme)) {
      return s3ObjectIO;
    }
    if ("gcs".equals(scheme)) {
      return gcsObjectIO;
    }
    if ("adls".equals(scheme)) {
      return adlsObjectIO;
    }
    if ("file".equals(scheme) || scheme == null) {
      return localObjectIO;
    }
    throw new IllegalArgumentException("Unknown scheme: " + scheme);
  }
}
