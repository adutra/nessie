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

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.catalog.service.server.AbstractIcebergCatalog;
import org.projectnessie.catalog.service.server.HeapS3MockResource;
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
    value = KeycloakTestResourceLifecycleManager.class)
@TestProfile(Profile.class)
public abstract class AbstractAuthEnabledTests extends AbstractIcebergCatalog {

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
      return Map.of("nessie.server.authentication.enabled", "true");
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return List.of(new TestResourceEntry(HeapS3MockResource.class));
    }
  }

  @Override
  protected String temporaryLocation() {
    // Rely on the bucket name to be "bucket" for HeapS3MockResource
    return "s3://bucket/" + UUID.randomUUID();
  }
}
