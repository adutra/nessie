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
package org.projectnessie.catalog.service.server.auth;

import static org.projectnessie.catalog.service.server.ObjectStorageMockTestResourceLifecycleManager.INIT_ADDRESS;
import static org.projectnessie.catalog.service.server.ObjectStorageMockTestResourceLifecycleManager.S3_INIT_ADDRESS;
import static org.projectnessie.catalog.service.server.ObjectStorageMockTestResourceLifecycleManager.S3_WAREHOUSE_LOCATION;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.catalog.service.server.AbstractIcebergCatalogTests;
import org.projectnessie.catalog.service.server.ObjectStorageMockTestResourceLifecycleManager;
import org.projectnessie.catalog.service.server.auth.AbstractAuthEnabledTests.Profile;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticationProvider;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticatorConfig;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakClientId;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakClientSecret;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakTokenEndpointUri;

@QuarkusTestResource(
    restrictToAnnotatedClass = true,
    parallel = true,
    value = KeycloakTestResourceLifecycleManager.class)
@TestProfile(Profile.class)
public abstract class AbstractAuthEnabledTests extends AbstractIcebergCatalogTests {

  @KeycloakTokenEndpointUri protected URI tokenEndpoint;

  @KeycloakClientId protected String clientId;

  @KeycloakClientSecret protected String clientSecret;

  HeapStorageBucket heapStorageBucket;

  @BeforeEach
  public void clearBucket() {
    heapStorageBucket.clear();
  }

  @Override
  protected NessieClientBuilder nessieClientBuilder() {
    return super.nessieClientBuilder()
        .withAuthentication(
            OAuth2AuthenticationProvider.create(
                OAuth2AuthenticatorConfig.builder()
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .tokenEndpoint(tokenEndpoint)
                    .build()));
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "nessie.server.authentication.enabled",
          "true",
          "nessie.catalog.default-warehouse.name",
          "warehouse",
          "nessie.catalog.default-warehouse.location",
          S3_WAREHOUSE_LOCATION);
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return Collections.singletonList(
          new TestResourceEntry(
              ObjectStorageMockTestResourceLifecycleManager.class,
              ImmutableMap.of(INIT_ADDRESS, S3_INIT_ADDRESS),
              true));
    }
  }

  @Override
  protected String temporaryLocation() {
    return S3_WAREHOUSE_LOCATION + "/temp/" + UUID.randomUUID();
  }
}
