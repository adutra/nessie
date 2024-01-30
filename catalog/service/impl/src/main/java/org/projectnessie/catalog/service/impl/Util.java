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

import static org.projectnessie.nessie.tasks.api.TaskState.failureState;
import static org.projectnessie.nessie.tasks.api.TaskState.retryableErrorState;

import java.util.Optional;
import java.util.function.Function;
import org.projectnessie.catalog.files.api.BackendThrottledException;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.versioned.storage.common.persist.ObjId;

final class Util {
  private Util() {}

  static ObjId nessieIdToObjId(NessieId id) {
    return ObjId.objIdFromByteArray(id.idAsBytes());
  }

  static NessieId objIdToNessieId(ObjId id) {
    return NessieId.nessieIdFromBytes(id.asByteArray());
  }

  static TaskState throwableAsErrorTaskState(Throwable throwable) {
    return anyCauseMatches(
            throwable,
            e -> {
              if (e instanceof BackendThrottledException) {
                BackendThrottledException throttledException = (BackendThrottledException) e;
                return retryableErrorState(throttledException.retryNotBefore(), e.toString());
              }
              return null;
            })
        .orElseGet(() -> failureState(throwable.toString()));
  }

  static <R> Optional<R> anyCauseMatches(
      Throwable throwable, Function<Throwable, R> mappingPredicate) {
    if (throwable == null) {
      return Optional.empty();
    }
    for (Throwable e = throwable; e != null; e = e.getCause()) {
      R r = mappingPredicate.apply(e);
      if (r != null) {
        return Optional.of(r);
      }
      for (Throwable sup : e.getSuppressed()) {
        r = mappingPredicate.apply(sup);
        if (r != null) {
          return Optional.of(r);
        }
      }
    }
    return Optional.empty();
  }
}
