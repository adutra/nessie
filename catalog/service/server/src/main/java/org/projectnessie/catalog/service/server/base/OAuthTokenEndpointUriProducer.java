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
package org.projectnessie.catalog.service.server.base;

import io.quarkus.arc.DefaultBean;
import io.quarkus.oidc.OidcTenantConfig;
import io.quarkus.oidc.common.runtime.OidcCommonConfig;
import io.quarkus.oidc.common.runtime.OidcCommonUtils;
import io.quarkus.oidc.runtime.OidcConfig;
import io.quarkus.runtime.TlsConfig;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.projectnessie.catalog.service.rest.TokenEndpointUri;

public class OAuthTokenEndpointUriProducer {

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  OidcConfig oidcConfig;

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  TlsConfig tlsConfig;

  @Inject Vertx vertx;

  @Produces
  @Singleton
  @TokenEndpointUri
  @DefaultBean
  public Optional<URI> produceOAuthTokenEndpointUri() {
    OidcTenantConfig tenantConfig = oidcConfig.defaultTenant;
    if (tenantConfig == null || !tenantConfig.isTenantEnabled()) {
      return Optional.empty();
    }
    String authServerUri = OidcCommonUtils.getAuthServerUrl(tenantConfig);
    String tokenUri = OidcCommonUtils.getOidcEndpointUrl(authServerUri, tenantConfig.tokenPath);
    if (tokenUri == null && tenantConfig.discoveryEnabled.orElse(true)) {
      long connectionDelayMillis = OidcCommonUtils.getConnectionDelayInMillis(tenantConfig);
      WebClient webClient = createWebClient(tenantConfig);
      try {
        tokenUri =
            OidcCommonUtils.discoverMetadata(
                    webClient,
                    OidcCommonUtils.getOidcRequestFilters(),
                    null,
                    authServerUri,
                    connectionDelayMillis,
                    vertx,
                    true)
                .onItem()
                .transform(json -> json.getString("token_endpoint"))
                .await()
                .atMost(Duration.ofSeconds(30));
      } finally {
        webClient.close();
      }
    }
    if (tokenUri == null) {
      return Optional.empty();
    }
    return Optional.of(URI.create(tokenUri));
  }

  private WebClient createWebClient(OidcCommonConfig tenantConfig) {
    WebClientOptions options = new WebClientOptions();
    OidcCommonUtils.setHttpClientOptions(tenantConfig, tlsConfig, options);
    return WebClient.create(vertx, options);
  }
}
