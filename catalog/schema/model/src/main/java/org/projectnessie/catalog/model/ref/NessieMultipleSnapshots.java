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
package org.projectnessie.catalog.model.ref;

import java.util.Optional;
import java.util.stream.Stream;
import org.projectnessie.nessie.immutables.NessieImmutable;

public interface NessieMultipleSnapshots {
  Stream<SnapshotResult> snapshots();

  @NessieImmutable
  interface SnapshotResult {
    NessieEntitySnapshotRef<?, ?> snapshot();

    default Optional<NessieTableSnapshotRef> asTable() {
      NessieEntitySnapshotRef<?, ?> snap = snapshot();
      return Optional.ofNullable(
          snap instanceof NessieTableSnapshotRef ? (NessieTableSnapshotRef) snap : null);
    }

    default Optional<NessieViewSnapshotRef> asView() {
      NessieEntitySnapshotRef<?, ?> snap = snapshot();
      return Optional.ofNullable(
          snap instanceof NessieViewSnapshotRef ? (NessieViewSnapshotRef) snap : null);
    }
  }
}
