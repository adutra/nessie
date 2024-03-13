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
package org.projectnessie.catalog.service.server.base;

import static java.time.Clock.systemUTC;

import io.smallrye.context.SmallRyeManagedExecutor;
import io.smallrye.context.SmallRyeThreadContext;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.context.ThreadContext;
import org.projectnessie.catalog.files.ResolvingObjectIO;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.s3.S3Clients;
import org.projectnessie.catalog.files.s3.S3Signer;
import org.projectnessie.catalog.files.secrets.SecretsProvider;
import org.projectnessie.catalog.service.common.config.CatalogServerConfig;
import org.projectnessie.catalog.service.common.config.ImmutableCatalogServerConfig;
import org.projectnessie.catalog.service.server.config.CatalogS3Config;
import org.projectnessie.catalog.service.server.config.CatalogSecrets;
import org.projectnessie.catalog.service.server.config.CatalogServiceConfig;
import org.projectnessie.nessie.tasks.async.TasksAsync;
import org.projectnessie.nessie.tasks.async.pool.JavaPoolTasksAsync;
import org.projectnessie.nessie.tasks.async.wrapping.ThreadContextTasksAsync;
import org.projectnessie.nessie.tasks.service.TasksServiceConfig;
import org.projectnessie.nessie.tasks.service.impl.TasksServiceExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * "Quick and dirty" producers providing connection to Nessie, a "storage" impl and object-store
 * I/O.
 */
public class CatalogProducers {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogProducers.class);

  @Produces
  @Singleton
  public CatalogServerConfig catalogServerConfig() {
    return ImmutableCatalogServerConfig.builder().sendStacktraceToClient(true).build();
  }

  @Produces
  @Singleton
  public S3Client s3Client(CatalogS3Config s3config) {
    return S3Clients.createS3BaseClient(s3config);
  }

  @Produces
  @Singleton
  public SecretsProvider secretsProvider(CatalogSecrets secrets) {
    return new SecretsProvider() {
      @Override
      public String getSecret(String secret) {
        String value = secrets.secrets().get(secret);
        if (value != null) {
          return value;
        }
        throw new IllegalArgumentException(
            "Secret '"
                + secret
                + "' is not defined, configure it using the configuration option 'nessie.catalog.secrets."
                + secret
                + "'.");
      }
    };
  }

  @Produces
  @Singleton
  public ObjectIO objectIO(
      CatalogS3Config s3config, S3Client s3client, SecretsProvider secretsProvider) {
    s3client = S3Clients.configuredClient(s3client, s3config, secretsProvider);
    return new ResolvingObjectIO(s3client, s3config);
  }

  @Produces
  @Singleton
  public RequestSigner signer(CatalogS3Config s3config, SecretsProvider secretsProvider) {
    return new S3Signer(s3config, secretsProvider);
  }

  /**
   * Provides the {@link TasksAsync} instance backed by a thread-pool executor configured according
   * to {@link CatalogServiceConfig}, with thread-context propagation.
   */
  @Produces
  @Singleton
  @TasksServiceExecutor
  public TasksAsync tasksAsync(ThreadContext threadContext, CatalogServiceConfig config) {
    int maxThreads = config.tasksMaxThreads();
    if (maxThreads <= 0) {
      // Keep the max pool size between 2 and 16
      maxThreads = Math.min(16, Math.max(2, Runtime.getRuntime().availableProcessors()));
    }

    ScheduledThreadPoolExecutor executorService =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactory() {
              private final ThreadGroup group = Thread.currentThread().getThreadGroup();
              private final AtomicInteger num = new AtomicInteger();

              @Override
              public Thread newThread(Runnable r) {
                return new Thread(group, r, "tasks-async-" + num.incrementAndGet());
              }
            });
    executorService.allowCoreThreadTimeOut(true);
    executorService.setKeepAliveTime(
        config.tasksThreadsKeepAlive().toMillis(), TimeUnit.MILLISECONDS);
    executorService.setMaximumPoolSize(maxThreads);

    LOGGER.info("Tasks handling configured with max {} threads.", maxThreads);
    LOGGER.debug(
        "Tasks handling configured with minimum delay of {}ms, race min/max of {}ms/{}ms.",
        config.tasksMinimumDelay().toMillis(),
        config.raceWaitMin(),
        config.raceWaitMax());

    TasksAsync base =
        new JavaPoolTasksAsync(executorService, systemUTC(), config.tasksMinimumDelay().toMillis());
    return new ThreadContextTasksAsync(base, threadContext);

    // Cannot use VertxTasksAsync :( - but using ThreadContext.contextual*() works fine.
    //    return new VertxTasksAsync(vertx, systemUTC(), 1L);
    //
    // With Vert.x Quarkus runs into this warning quite often:
    //   WARN  [io.qua.ope.run.QuarkusContextStorage] (executor-thread-1) Context in storage not the
    //     expected context, Scope.close was not called correctly. Details: OTel context before:
  }

  @Produces
  @Singleton
  public TasksServiceConfig tasksServiceConfig(CatalogServiceConfig config) {
    return TasksServiceConfig.tasksServiceConfig(
        "tasks", config.raceWaitMin().toMillis(), config.raceWaitMax().toMillis());
  }

  /** Provides the executor to run actual catalog import jobs, with thread-context propagation. */
  @Produces
  @Singleton
  @Named("import-jobs")
  public Executor importJobExecutor(ThreadContext threadContext, CatalogServiceConfig config) {
    ExecutorService executor =
        SmallRyeManagedExecutor.newThreadPoolExecutor(config.maxConcurrentImports(), -1);
    return new SmallRyeManagedExecutor(
        config.maxConcurrentImports(),
        -1,
        (SmallRyeThreadContext) threadContext,
        executor,
        "import-jobs");
  }
}
