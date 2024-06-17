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
package org.projectnessie.client.auth.oauth2;

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_AUDIENCE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_RESOURCE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CURRENT_ACCESS_TOKEN;
import static org.projectnessie.client.NessieConfigConstants.CURRENT_REFRESH_TOKEN;
import static org.projectnessie.client.NessieConfigConstants.NO_TOKEN;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientConfig.applyConfigOption;
import static org.projectnessie.client.auth.oauth2.TypedToken.URN_ACCESS_TOKEN;
import static org.projectnessie.client.auth.oauth2.TypedToken.URN_REFRESH_TOKEN;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.immutables.value.Value;
import org.projectnessie.client.NessieConfigConstants;

/**
 * Configuration for OAuth2 token exchange.
 *
 * <p>STILL IN BETA. API MAY CHANGE.
 */
@Value.Immutable
public interface TokenExchangeConfig {

  static TokenExchangeConfig fromConfigSupplier(Function<String, String> config) {
    TokenExchangeConfig.Builder builder = TokenExchangeConfig.builder();
    applyConfigOption(
        config, CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_RESOURCE, builder::resource, URI::create);
    applyConfigOption(config, CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_AUDIENCE, builder::audience);

    String subjectToken = config.apply(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN);
    String actorToken = config.apply(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN);

    Optional<URI> subjectTokenType =
        Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN_TYPE))
            .map(URI::create);
    Optional<URI> actorTokenType =
        Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN_TYPE))
            .map(URI::create);

    if (subjectToken == null || subjectToken.equalsIgnoreCase(CURRENT_ACCESS_TOKEN)) {
      builder.subjectTokenProvider(
          (accessToken, refreshToken) ->
              TypedToken.of(accessToken, subjectTokenType.orElse(URN_ACCESS_TOKEN)));
    } else if (subjectToken.equalsIgnoreCase(CURRENT_REFRESH_TOKEN)) {
      builder.subjectTokenProvider(
          (accessToken, refreshToken) ->
              TypedToken.of(refreshToken, subjectTokenType.orElse(URN_REFRESH_TOKEN)));
    } else {
      builder.subjectToken(TypedToken.of(subjectToken, subjectTokenType.orElse(URN_ACCESS_TOKEN)));
    }

    if (actorToken != null && !actorToken.equalsIgnoreCase(NO_TOKEN)) {
      if (actorToken.equalsIgnoreCase(CURRENT_ACCESS_TOKEN)) {
        builder.actorTokenProvider(
            (accessToken, refreshToken) ->
                TypedToken.of(accessToken, actorTokenType.orElse(URN_ACCESS_TOKEN)));
      } else if (actorToken.equalsIgnoreCase(CURRENT_REFRESH_TOKEN)) {
        builder.actorTokenProvider(
            (accessToken, refreshToken) ->
                refreshToken == null
                    ? null
                    : TypedToken.of(refreshToken, actorTokenType.orElse(URN_REFRESH_TOKEN)));
      } else {
        builder.actorToken(TypedToken.of(actorToken, actorTokenType.orElse(URN_ACCESS_TOKEN)));
      }
    }
    return builder.build();
  }

  /**
   * The type of the requested security token. By default, {@link TypedToken#URN_ACCESS_TOKEN}.
   *
   * <p>Currently, it is not possible to request any other token type, so this property is not
   * configurable through system properties.
   */
  @Value.Default
  default URI getRequestedTokenType() {
    return URN_ACCESS_TOKEN;
  }

  /**
   * A URI that indicates the target service or resource where the client intends to use the
   * requested security token.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_RESOURCE
   */
  Optional<URI> getResource();

  /**
   * The logical name of the target service where the client intends to use the requested security
   * token. This serves a purpose similar to the resource parameter but with the client providing a
   * logical name for the target service.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_AUDIENCE
   */
  Optional<String> getAudience();

  /**
   * The subject token provider. The provider will be invoked with the current access token (never
   * null) and the current refresh token, or null if none available; and should return a {@link
   * TypedToken} representing the subject token. It must NOT return null.
   *
   * <p>By default, the provider will return the access token itself. This should be suitable for
   * most cases.
   *
   * <p>This property cannot be set through configuration, but only programmatically. The
   * configuration exposes two options: the subject token and its type. These options allow to pass
   * a static subject token only.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN_TYPE
   */
  @Value.Default
  @Value.Auxiliary
  default BiFunction<AccessToken, RefreshToken, TypedToken> getSubjectTokenProvider() {
    return (accessToken, refreshToken) -> TypedToken.of(accessToken);
  }

  /**
   * The actor token provider. The provider will be invoked with the current access token (never
   * null) and the current refresh token, or null if none available; and should return a {@link
   * TypedToken} representing the actor token. If the provider returns null, then no actor token
   * will be used.
   *
   * <p>Actor tokens are useful in delegation scenarios. By default, no actor token is used.
   *
   * <p>This property cannot be set through configuration, but only programmatically. The
   * configuration exposes two options: the actor token and its type. These options allow to pass a
   * static actor token only.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN_TYPE
   */
  @Value.Default
  @Value.Auxiliary
  default BiFunction<AccessToken, RefreshToken, TypedToken> getActorTokenProvider() {
    return (accessToken, refreshToken) -> null;
  }

  static TokenExchangeConfig.Builder builder() {
    return ImmutableTokenExchangeConfig.builder();
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder requestedTokenType(URI tokenType);

    @CanIgnoreReturnValue
    Builder resource(URI resource);

    @CanIgnoreReturnValue
    Builder audience(String audience);

    @CanIgnoreReturnValue
    Builder subjectTokenProvider(BiFunction<AccessToken, RefreshToken, TypedToken> provider);

    @CanIgnoreReturnValue
    Builder actorTokenProvider(BiFunction<AccessToken, RefreshToken, TypedToken> provider);

    @CanIgnoreReturnValue
    default Builder subjectToken(TypedToken token) {
      return subjectTokenProvider((accessToken, refreshToken) -> token);
    }

    @CanIgnoreReturnValue
    default Builder actorToken(TypedToken token) {
      return actorTokenProvider((accessToken, refreshToken) -> token);
    }

    TokenExchangeConfig build();
  }
}
