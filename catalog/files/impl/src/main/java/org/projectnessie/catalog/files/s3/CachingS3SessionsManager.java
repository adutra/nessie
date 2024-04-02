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
package org.projectnessie.catalog.files.s3;

import static java.util.Collections.singletonList;
import static org.projectnessie.catalog.files.s3.S3Clients.basicCredentialsProvider;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.checkerframework.checker.index.qual.NonNegative;
import org.projectnessie.catalog.files.secrets.SecretsProvider;
import org.projectnessie.nessie.immutables.NessieImmutable;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/** Maintains a pool of STS clients and manages refreshing session credentials on demand. */
public class CachingS3SessionsManager {
  public static final String CLIENTS_CACHE_NAME = "sts-clients";
  public static final String SESSIONS_CACHE_NAME = "sts-sessions";

  private final Cache<StsClientKey, StsClient> clients;
  private final LoadingCache<SessionKey, Credentials> sessions;
  private final Function<StsClientKey, StsClient> clientBuilder;
  private final Duration expiryReduction;
  private final SecretsProvider secretsProvider;
  private final BiFunction<StsClient, SessionKey, Credentials> sessionCredentialsFetcher;

  public CachingS3SessionsManager(
      S3Options<?> options,
      SdkHttpClient sdkClient,
      MeterRegistry meterRegistry,
      SecretsProvider secretsProvider) {
    this(
        options,
        System::currentTimeMillis,
        (parameters) -> client(parameters, sdkClient),
        Optional.of(meterRegistry),
        secretsProvider,
        null);
  }

  @VisibleForTesting
  CachingS3SessionsManager(
      S3Options<?> options,
      LongSupplier systemTimeMillis,
      Function<StsClientKey, StsClient> clientBuilder,
      Optional<MeterRegistry> meterRegistry,
      SecretsProvider secretsProvider,
      BiFunction<StsClient, SessionKey, Credentials> sessionCredentialsFetcher) {
    this.clientBuilder = clientBuilder;
    this.expiryReduction = options.effectiveSessionCredentialRefreshGracePeriod();
    this.secretsProvider = secretsProvider;
    this.sessionCredentialsFetcher =
        sessionCredentialsFetcher != null
            ? sessionCredentialsFetcher
            : this::fetchSessionCredentials;

    // Cache clients without expiration time keyed by the parameters that cannot be adjusted
    // per-request. Note: Credentials are set individually in each STS request.
    this.clients =
        Caffeine.newBuilder()
            .maximumSize(options.effectiveStsClientsCacheMaxEntries())
            .recordStats(
                () ->
                    statsCounter(
                        meterRegistry,
                        CLIENTS_CACHE_NAME,
                        options.effectiveStsClientsCacheMaxEntries()))
            .build();

    this.sessions =
        Caffeine.newBuilder()
            .ticker(() -> TimeUnit.MILLISECONDS.toNanos(systemTimeMillis.getAsLong()))
            .maximumSize(options.effectiveSessionCredentialCacheMaxEntries())
            .recordStats(
                () ->
                    statsCounter(
                        meterRegistry,
                        SESSIONS_CACHE_NAME,
                        options.effectiveSessionCredentialCacheMaxEntries()))
            .expireAfter(
                new Expiry<SessionKey, Credentials>() {
                  @Override
                  public long expireAfterCreate(
                      SessionKey key, Credentials value, long currentTimeNanos) {
                    return lifetimeNanos(value, currentTimeNanos);
                  }

                  @Override
                  public long expireAfterUpdate(
                      SessionKey key,
                      Credentials value,
                      long currentTime,
                      @NonNegative long currentDuration) {
                    return currentDuration;
                  }

                  @Override
                  public long expireAfterRead(
                      SessionKey key,
                      Credentials value,
                      long currentTime,
                      @NonNegative long currentDuration) {
                    return currentDuration;
                  }
                })
            .build(this::loadSession);
  }

  private StatsCounter statsCounter(
      Optional<MeterRegistry> meterRegistry, String name, int maxSize) {
    if (meterRegistry.isPresent()) {
      meterRegistry
          .get()
          .gauge("max_entries", singletonList(Tag.of("cache", name)), "", x -> maxSize);

      return new CaffeineStatsCounter(meterRegistry.get(), name);
    }

    return StatsCounter.disabledStatsCounter();
  }

  private long lifetimeNanos(Credentials credentials, long currentTimeNanos) {
    Instant expiration = credentials.expiration();
    long currentTimeMillis = TimeUnit.NANOSECONDS.toMillis(currentTimeNanos);
    currentTimeMillis += expiryReduction.toMillis();
    Instant effectiveNow = Instant.ofEpochMilli(currentTimeMillis);

    long lifetimeMillis;
    if (expiration.isBefore(effectiveNow)) {
      lifetimeMillis = 0;
    } else {
      lifetimeMillis = effectiveNow.until(expiration, ChronoUnit.MILLIS);
    }
    return TimeUnit.MILLISECONDS.toNanos(lifetimeMillis);
  }

  private Credentials loadSession(SessionKey sessionKey) {
    // Note: StsClients may be shared across repositories.
    StsClientKey clientKey =
        ImmutableStsClientKey.of(
            sessionKey.stsEndpoint(),
            sessionKey
                .region()
                .orElseThrow(() -> new IllegalArgumentException("S3 region must be provided")));
    StsClient client = clients.get(clientKey, clientBuilder);

    return sessionCredentialsFetcher.apply(client, sessionKey);
  }

  private Credentials fetchSessionCredentials(StsClient client, SessionKey sessionKey) {
    AssumeRoleRequest.Builder request = AssumeRoleRequest.builder();
    request.roleSessionName(
        sessionKey.roleSessionName().orElse(S3BucketOptions.DEFAULT_SESSION_NAME));
    request.roleArn(
        sessionKey
            .roleArn()
            .orElseThrow(() -> new IllegalArgumentException("Role ARN must be configured")));
    sessionKey.externalId().ifPresent(request::externalId);
    sessionKey.iamPolicy().ifPresent(request::policy);

    request.overrideConfiguration(
        builder ->
            builder.credentialsProvider(
                basicCredentialsProvider(
                    sessionKey.accessKeyIdRef(),
                    sessionKey.secretAccessKeyRef(),
                    secretsProvider)));

    AssumeRoleResponse response = client.assumeRole(request.build());
    return response.credentials();
  }

  Credentials sessionCredentials(String repositoryId, S3BucketOptions options) {
    // Client parameters are part of the credential's key because clients in different regions may
    // issue different credentials.
    SessionKey sessionKey = buildSessionKey(repositoryId, options);

    return sessions.get(sessionKey);
  }

  private static SessionKey buildSessionKey(String repositoryId, S3BucketOptions options) {
    return ImmutableSessionKey.builder()
        .repositoryId(repositoryId)
        .accessKeyIdRef(options.accessKeyIdRef())
        .secretAccessKeyRef(options.secretAccessKeyRef())
        .region(options.region())
        .stsEndpoint(options.stsEndpoint())
        .roleArn(options.roleArn())
        .iamPolicy(options.iamPolicy())
        .roleSessionName(options.roleSessionName())
        .externalId(options.externalId())
        .build();
  }

  private static StsClient client(StsClientKey parameters, SdkHttpClient sdkClient) {
    StsClientBuilder builder = StsClient.builder();
    builder.httpClient(sdkClient);
    parameters.endpoint().ifPresent(builder::endpointOverride);
    builder.region(Region.of(parameters.region()));
    return builder.build();
  }

  @NessieImmutable
  interface SessionKey {
    String repositoryId();

    Optional<URI> stsEndpoint();

    Optional<String> accessKeyIdRef();

    Optional<String> secretAccessKeyRef();

    Optional<String> region();

    Optional<String> roleArn();

    Optional<String> iamPolicy();

    Optional<String> roleSessionName();

    Optional<String> externalId();
  }

  @NessieImmutable
  interface StsClientKey {
    Optional<URI> endpoint();

    String region();
  }
}
