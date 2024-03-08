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
package org.projectnessie.catalog.formats.iceberg.meta;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Collections;
import java.util.List;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergSortOrder.class)
@JsonDeserialize(as = ImmutableIcebergSortOrder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergSortOrder {

  static Builder builder() {
    return ImmutableIcebergSortOrder.builder();
  }

  static IcebergSortOrder sortOrder(int orderId, List<IcebergSortField> fields) {
    return ImmutableIcebergSortOrder.of(orderId, fields);
  }

  IcebergSortOrder UNSORTED_ORDER = sortOrder(0, Collections.emptyList());

  static IcebergSortOrder unsorted() {
    return UNSORTED_ORDER;
  }

  int orderId();

  List<IcebergSortField> fields();

  interface Builder {
    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder from(IcebergSortOrder sortOrder);

    @CanIgnoreReturnValue
    Builder orderId(int orderId);

    @CanIgnoreReturnValue
    Builder addField(IcebergSortField element);

    @CanIgnoreReturnValue
    Builder addFields(IcebergSortField... elements);

    @CanIgnoreReturnValue
    Builder fields(Iterable<? extends IcebergSortField> elements);

    @CanIgnoreReturnValue
    Builder addAllFields(Iterable<? extends IcebergSortField> elements);

    IcebergSortOrder build();
  }
}
