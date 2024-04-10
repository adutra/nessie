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
package org.projectnessie.catalog.service.server;

import static org.projectnessie.catalog.service.server.ObjectStorageMockTestResourceLifecycleManager.S3_WAREHOUSE_LOCATION;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures;
import org.projectnessie.catalog.service.server.tests.AbstractNessieCoreTests;
import org.projectnessie.objectstoragemock.HeapStorageBucket;

@QuarkusTest
@TestProfile(value = S3UnitTestProfile.class)
public class TestNessieCore extends AbstractNessieCoreTests {

  @Inject ObjectIO objectIO;

  String currentBase;

  HeapStorageBucket heapStorageBucket;

  @BeforeEach
  public void clearBucket() {
    heapStorageBucket.clear();
  }

  @BeforeEach
  protected void setup() {
    currentBase = S3_WAREHOUSE_LOCATION + "/" + UUID.randomUUID() + "/";
  }

  @Override
  protected String basePath() {
    return currentBase;
  }

  @Override
  protected IcebergGenerateFixtures.ObjectWriter objectWriter() {
    return (name, data) -> {
      URI location =
          name.isAbsolute() ? name : URI.create(currentBase + "/" + name.getPath()).normalize();
      try (OutputStream output = objectIO.writeObject(location)) {
        output.write(data);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return location.toString();
    };
  }
}
