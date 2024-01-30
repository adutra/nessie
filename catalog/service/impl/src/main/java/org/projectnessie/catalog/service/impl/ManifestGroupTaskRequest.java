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

import static org.projectnessie.catalog.service.impl.ManifestGroupObj.OBJ_TYPE;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import org.immutables.value.Value;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.nessie.tasks.api.TaskBehavior;
import org.projectnessie.nessie.tasks.api.TaskRequest;
import org.projectnessie.nessie.tasks.api.Tasks;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;

@NessieImmutable
public interface ManifestGroupTaskRequest
    extends TaskRequest<ManifestGroupObj, ManifestGroupObj.Builder> {
  @Override
  @Value.NonAttribute
  default ObjType objType() {
    return OBJ_TYPE;
  }

  @Override
  @Value.NonAttribute
  default TaskBehavior<ManifestGroupObj, ManifestGroupObj.Builder> behavior() {
    return ManifestGroupTaskBehavior.INSTANCE;
  }

  @Override
  ObjId objId();

  EntitySnapshotObj snapshotObj();

  @Value.Auxiliary
  Persist persist();

  @Value.Auxiliary
  ObjectIO objectIO();

  @Value.Auxiliary
  Executor executor();

  @Value.Auxiliary
  Tasks tasks();

  @Override
  @Value.NonAttribute
  default CompletionStage<ManifestGroupObj.Builder> submitExecution() {
    return CompletableFuture.supplyAsync(
        new ImportManifestGroupWorker(this)::importManifestGroup, executor());
  }

  static ManifestGroupTaskRequest manifestGroupTaskRequest(
      ObjId objId,
      EntitySnapshotObj snapshotObj,
      Persist persist,
      ObjectIO objectIO,
      Executor executor,
      Tasks tasks) {
    return ImmutableManifestGroupTaskRequest.of(
        objId, snapshotObj, persist, objectIO, executor, tasks);
  }
}
