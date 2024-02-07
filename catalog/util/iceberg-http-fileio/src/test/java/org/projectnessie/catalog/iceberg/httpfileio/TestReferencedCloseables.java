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
package org.projectnessie.catalog.iceberg.httpfileio;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestReferencedCloseables {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void closeablesSelfReference() {
    AutoCloseable closeable = () -> {};
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ReferencedCloseables.addCloseable(closeable, closeable));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 10, 1000})
  public void closeablesPerReferent(int numCloseables) throws Exception {

    List<CompletableFuture<Object>> futures = new ArrayList<>();
    List<AutoCloseable> closeables = new ArrayList<>();
    for (int i = 0; i < numCloseables; i++) {
      CompletableFuture<Object> future = new CompletableFuture<>();
      closeables.add(() -> future.complete("x"));
      futures.add(future);
    }

    Object referent = new StringBuilder();

    for (AutoCloseable closeable : closeables) {
      ReferencedCloseables.addCloseable(referent, closeable);
    }

    CompletableFuture<Void> future =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    // Nonsense assertion, but keeps 'referent' variable in scope.
    soft.assertThat(referent).isNotNull();

    soft.assertThat(futures).noneMatch(CompletableFuture::isDone);

    referent = null;

    for (int i = 0; i < 5; i++) {
      System.gc();

      try {
        future.get(500L, TimeUnit.MILLISECONDS);
        break;
      } catch (TimeoutException to) {
        // continue
      }
    }

    soft.assertThatCode(() -> future.get(250L, TimeUnit.MILLISECONDS)).doesNotThrowAnyException();

    // Nonsense assertion, but keeps 'referent' variable in scope.
    soft.assertThat(referent).isNull();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 10, 1000})
  public void manyReferents(int numReferents) throws Exception {

    List<CompletableFuture<Object>> futures = new ArrayList<>();
    List<AutoCloseable> closeables = new ArrayList<>();
    for (int i = 0; i < numReferents; i++) {
      CompletableFuture<Object> future = new CompletableFuture<>();
      closeables.add(() -> future.complete("x"));
      futures.add(future);
    }

    List<Object> referents = new ArrayList<>();
    for (AutoCloseable closeable : closeables) {
      Object referent = new StringBuilder();
      referents.add(referent);
      ReferencedCloseables.addCloseable(referent, closeable);
    }

    CompletableFuture<Void> future =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    soft.assertThat(futures).noneMatch(CompletableFuture::isDone);

    referents.clear();

    for (int i = 0; i < 5; i++) {
      System.gc();

      try {
        future.get(500L, TimeUnit.MILLISECONDS);
        break;
      } catch (TimeoutException to) {
        // continue
      }
    }

    soft.assertThatCode(() -> future.get(250L, TimeUnit.MILLISECONDS)).doesNotThrowAnyException();

    // Nonsense assertion, but keeps 'referent' variable referenced.
    soft.assertThat(referents).isEmpty();
  }
}
