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

import static org.projectnessie.catalog.service.server.ObjectStorageMockTestResourceLifecycleManager.GCS_WAREHOUSE_LOCATION;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GcsUnitTestProfile implements QuarkusTestProfile {

  @Override
  public List<TestResourceEntry> testResources() {
    return Collections.singletonList(
        new TestResourceEntry(ObjectStorageMockTestResourceLifecycleManager.class));
  }

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .put("nessie.catalog.warehouses.warehouse1.name", "warehouse1")
        .put("nessie.catalog.warehouses.warehouse1.location", GCS_WAREHOUSE_LOCATION)
        .build();
  }
}
