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

import java.util.Map;
import java.util.Set;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.model.ContentKey;

public interface NessieTransaction {

  NessieTransaction rename(ContentKey from, ContentKey to);

  NessieTransaction delete(ContentKey key);

  NessieCreateEntity createNew(ContentKey key);

  NessieUpdateEntity update(ContentKey key);

  NessieCatalogSnapshot commit();

  interface NessieCreateEntity {
    NessieCreateEntity tableFormat(TableFormat tableFormat);

    NessieCreateTable table();

    NessieCreateView view();

    NessieCreateNamespace namespace();
  }

  interface NessieUpdateEntity {
    NessieUpdateTable table();

    NessieUpdateView view();

    NessieUpdateNamespace namespace();
  }

  interface NessieCreateBase<U extends NessieCreateBase<U>> {
    NessieCatalogSnapshot create();
  }

  interface NessieCreateProperties<U extends NessieCreateBase<U>> extends NessieCreateBase<U> {
    U addProperty(String key, String value);

    U addProperties(Map<String, String> properties);
  }

  interface NessieCreateTable
      extends NessieCreateProperties<NessieCreateTable>, NessieCreateBase<NessieCreateTable> {
    NessieCreateTable addSchema(NessieSchema schema);

    NessieCreateTable addPartitionDefinition(NessiePartitionDefinition partitionDefinition);

    NessieCreateTable addSortDefinition(NessieSortDefinition sortDefinition);

    NessieCreateTable currentSchema(NessieSchema schema);

    NessieCreateTable currentPartitionDefinition(NessiePartitionDefinition partitionDefinition);

    NessieCreateTable currentSortDefinition(NessieSortDefinition sortDefinition);
  }

  interface NessieCreateView
      extends NessieCreateProperties<NessieCreateView>, NessieCreateBase<NessieCreateView> {
    NessieCreateView addSchema(NessieSchema schema);

    NessieCreateView currentSchema(NessieSchema schema);
  }

  interface NessieCreateNamespace
      extends NessieCreateProperties<NessieCreateNamespace>,
          NessieCreateBase<NessieCreateNamespace> {}

  interface NessieUpdateBase<U extends NessieUpdateBase<U>> {
    NessieCatalogSnapshot update();
  }

  interface NessieUpdateProperties<U extends NessieUpdateBase<U>> extends NessieUpdateBase<U> {
    U addProperty(String key, String value);

    U addProperties(Map<String, String> properties);

    U removeProperty(String key);

    U removeProperties(Set<String> properties);
  }

  interface NessieUpdateTable
      extends NessieUpdateProperties<NessieUpdateTable>, NessieUpdateBase<NessieUpdateTable> {
    NessieUpdateTable addSchema(NessieSchema schema);

    NessieUpdateTable addPartitionDefinition(NessiePartitionDefinition partitionDefinition);

    NessieUpdateTable addSortDefinition(NessieSortDefinition sortDefinition);

    NessieUpdateTable currentSchema(NessieSchema schema);

    NessieUpdateTable currentPartitionDefinition(NessiePartitionDefinition partitionDefinition);

    NessieUpdateTable currentSortDefinition(NessieSortDefinition sortDefinition);
  }

  interface NessieUpdateView
      extends NessieUpdateProperties<NessieUpdateView>, NessieUpdateBase<NessieUpdateView> {
    NessieUpdateView addSchema(NessieSchema schema);

    NessieUpdateView currentSchema(NessieSchema schema);
  }

  interface NessieUpdateNamespace
      extends NessieUpdateProperties<NessieUpdateNamespace>,
          NessieUpdateBase<NessieUpdateNamespace> {}
}
