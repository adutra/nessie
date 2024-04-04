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
package org.projectnessie.catalog.service.server;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.projectnessie.objectstoragemock.HeapStorageBucket.newHeapStorageBucket;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import jakarta.enterprise.inject.Produces;
import java.util.Map;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.objectstoragemock.ObjectStorageMock;

public class HeapS3MockResource implements QuarkusTestResourceLifecycleManager {
  private ObjectStorageMock.MockServer mockServer;
  private HeapStorageBucket bucket;

  @Produces
  public ObjectStorageMock objectStorageMock() {
    // This makes Quarkus happy w/ the object-storage-mock resources
    throw new UnsupportedOperationException();
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(bucket, f -> f.getType().isInstance(bucket));
  }

  @Override
  public Map<String, String> start() {
    checkState(mockServer == null);

    String bucket = "bucket";
    this.bucket = newHeapStorageBucket();
    this.mockServer =
        ObjectStorageMock.builder().putBuckets(bucket, this.bucket.bucket()).build().start();

    return ImmutableMap.<String, String>builder()
        .put("nessie.catalog.default-warehouse.name", "warehouse")
        .put("nessie.catalog.default-warehouse.location", format("s3://%s/some/prefix/", bucket))
        .put("nessie.catalog.service.s3.cloud", "private")
        .put("nessie.catalog.service.s3.endpoint", mockServer.getS3BaseUri().toString())
        .put("nessie.catalog.service.s3.path-style-access", "true")
        .put("nessie.catalog.service.s3.region", "eu-central-1")
        .put("nessie.catalog.service.s3.access-key-id-ref", "awsAccessKeyId")
        .put("nessie.catalog.service.s3.secret-access-key-ref", "awsSecretAccessKey")
        .put("nessie.catalog.secrets.awsAccessKeyId", "accessKey")
        .put("nessie.catalog.secrets.awsSecretAccessKey", "secretKey")
        .build();
  }

  @Override
  public void stop() {
    if (mockServer != null) {
      try {
        mockServer.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mockServer = null;
      }
    }
  }
}
