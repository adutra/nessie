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

import static org.projectnessie.catalog.service.impl.ManifestObj.OBJ_TYPE;

import jakarta.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import org.immutables.value.Value;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.nessie.tasks.api.TaskBehavior;
import org.projectnessie.nessie.tasks.api.TaskRequest;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;

@NessieImmutable
public interface ManifestTaskRequest extends TaskRequest<ManifestObj, ManifestObj.Builder> {
  @Override
  @Value.NonAttribute
  default ObjType objType() {
    return OBJ_TYPE;
  }

  @Override
  @Value.NonAttribute
  default TaskBehavior<ManifestObj, ManifestObj.Builder> behavior() {
    return ManifestTaskBehavior.INSTANCE;
  }

  @Override
  ObjId objId();

  String manifestFileLocation();

  @Nullable
  IcebergManifestFile manifestFile();

  NessieTableSnapshot snapshot();

  @Value.Auxiliary
  Persist persist();

  @Value.Auxiliary
  ObjectIO objectIO();

  @Value.Auxiliary
  Executor executor();

  @Override
  @Value.NonAttribute
  default CompletionStage<ManifestObj.Builder> submitExecution() {
    return CompletableFuture.supplyAsync(
        () ->
            new ImportManifestFileWorker(this, snapshot())
                .importManifestFile(manifestFileLocation()),
        executor());
  }

  static ManifestTaskRequest manifestTaskRequest(
      ObjId objId,
      String manifestFileLocation,
      IcebergManifestFile manifestFile,
      NessieTableSnapshot snapshot,
      Persist persist,
      ObjectIO objectIO,
      Executor executor) {
    return ImmutableManifestTaskRequest.of(
        objId, manifestFileLocation, manifestFile, snapshot, persist, objectIO, executor);
  }
}
