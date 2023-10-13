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
package org.projectnessie.versioned.storage.common.logic;

import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;

public interface ConsistencyLogic {

  <R> R checkReference(Reference reference, CommitStatusCallback<R> callback);

  <R> R checkCommit(ObjId commitId, CommitStatusCallback<R> callback);

  @FunctionalInterface
  interface CommitStatusCallback<R> {
    R commitCallback(CommitStatus commitStatus);
  }

  @Value.Immutable
  interface CommitStatus {
    @Value.Parameter(order = 1)
    ObjId id();

    @Value.Parameter(order = 2)
    @Nullable
    @jakarta.annotation.Nullable
    CommitObj commit();

    @Value.Parameter(order = 3)
    boolean indexObjectsAvailable();

    @Value.Parameter(order = 4)
    boolean contentObjectsAvailable();

    static CommitStatus commitStatus(
        ObjId id,
        CommitObj commit,
        boolean indexObjectsAvailable,
        boolean contentObjectsAvailable) {
      return ImmutableCommitStatus.of(id, commit, indexObjectsAvailable, contentObjectsAvailable);
    }
  }
}
