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
package org.projectnessie.catalog.service.impl;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.nessie.tasks.api.TaskState.failureState;
import static org.projectnessie.nessie.tasks.api.TaskState.retryableErrorState;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.api.BackendThrottledException;
import org.projectnessie.nessie.tasks.api.TaskState;

@ExtendWith(SoftAssertionsExtension.class)
public class TestUtil {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void anyCauseMatches(Throwable throwable, Instant expected) {
    soft.assertThat(
            Util.anyCauseMatches(
                throwable,
                t ->
                    t instanceof BackendThrottledException
                        ? ((BackendThrottledException) t).retryNotBefore()
                        : null))
        .isEqualTo(Optional.ofNullable(expected));
  }

  @ParameterizedTest
  @MethodSource("anyCauseMatches")
  public void throwableAsErrorTaskState(Throwable throwable, Instant expected) {
    TaskState taskState = Util.throwableAsErrorTaskState(throwable);

    soft.assertThat(taskState)
        .isEqualTo(
            expected != null
                ? retryableErrorState(expected, BackendThrottledException.class.getName() + ": foo")
                : failureState(throwable.toString()));
  }

  static Stream<Arguments> anyCauseMatches() {
    Instant retryNotBefore = Instant.now().plus(42, MINUTES);
    BackendThrottledException throttled = new BackendThrottledException(retryNotBefore, "foo");

    Exception sup1 = new Exception();
    sup1.addSuppressed(throttled);

    Exception sup2 = new Exception();
    sup2.addSuppressed(new RuntimeException());
    sup2.addSuppressed(throttled);

    return Stream.of(
        arguments(throttled, retryNotBefore),
        arguments(new Exception(throttled), retryNotBefore),
        arguments(new Exception(new Exception(throttled)), retryNotBefore),
        arguments(new Exception(), null),
        arguments(new Exception(new Exception()), null),
        arguments(sup1, retryNotBefore),
        arguments(sup2, retryNotBefore),
        arguments(new Exception(sup1), retryNotBefore),
        arguments(new Exception(sup2), retryNotBefore));
  }
}
