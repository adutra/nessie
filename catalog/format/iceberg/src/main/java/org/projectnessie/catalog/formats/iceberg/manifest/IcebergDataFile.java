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
package org.projectnessie.catalog.formats.iceberg.manifest;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericData;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface IcebergDataFile {

  static Builder builder() {
    return ImmutableIcebergDataFile.builder();
  }

  @Value.Default
  default IcebergDataContent content() {
    return IcebergDataContent.DATA;
  }

  String filePath();

  IcebergFileFormat fileFormat();

  long recordCount();

  long fileSizeInBytes();

  @Nullable
  @jakarta.annotation.Nullable
  @Value.Default
  // Iceberg spec V1 only (not in V2)
  default Long blockSizeInBytes() {
    return 67108864L;
  }

  // TODO Iceberg uses the option 'write.metadata.metrics.max-inferred-column-defaults' (default:
  //  100), see org.apache.iceberg.TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, to limit
  //  statistics to that amount of columns (first columns by iteration order)

  // TODO Use Agrona-Collections
  Map<Integer, Long> columnSizes();

  // TODO Use Agrona-Collections
  Map<Integer, Long> valueCounts();

  // TODO Use Agrona-Collections
  Map<Integer, Long> nullValueCounts();

  // TODO Use Agrona-Collections
  Map<Integer, Long> nanValueCounts();

  // TODO subject to write.metadata.metrics.default / write.metadata.metrics.column.* settings !!
  //  Default is 'truncate(16)', see
  //  https://iceberg.apache.org/docs/1.3.0/configuration/#write-properties
  // TODO how is this encoded?? in IJ's Avro/Parquet viewer string values appear "interesting"
  // TODO Use Agrona-Collections
  Map<Integer, ByteBuffer> lowerBounds();

  // TODO subject to write.metadata.metrics.default / write.metadata.metrics.column.* settings !!
  //  Default is 'truncate(16)', see
  //  https://iceberg.apache.org/docs/1.3.0/configuration/#write-properties
  // TODO how is this encoded?? in IJ's Avro/Parquet viewer string values appear "interesting"
  // TODO Iceberg does not add some suffix like Delta does ("tie breaker") for the max value, but
  //  uses Parquet's functionality, see
  //  org.apache.parquet.internal.column.columnindex.BinaryTruncator.truncateMax?
  // TODO Use Agrona-Collections
  Map<Integer, ByteBuffer> upperBounds();

  @Nullable
  @jakarta.annotation.Nullable
  ByteBuffer keyMetadata();

  // TODO Use Agrona-Collections
  List<Long> splitOffsets();

  // TODO Use Agrona-Collections
  List<Integer> equalityIds();

  @Value.Default
  default int sortOrderId() {
    return 0;
  }

  @Nullable
  @jakarta.annotation.Nullable
  Integer specId();

  GenericData.Record partition();

  interface Builder {

    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder from(IcebergDataFile dataFile);

    @CanIgnoreReturnValue
    Builder content(@Nullable IcebergDataContent content);

    @CanIgnoreReturnValue
    Builder filePath(String filePath);

    @CanIgnoreReturnValue
    Builder fileFormat(IcebergFileFormat fileFormat);

    @CanIgnoreReturnValue
    Builder recordCount(long recordCount);

    @CanIgnoreReturnValue
    Builder fileSizeInBytes(long fileSizeInBytes);

    @CanIgnoreReturnValue
    Builder blockSizeInBytes(@Nullable Long blockSizeInBytes);

    @CanIgnoreReturnValue
    Builder putColumnSizes(int key, long value);

    @CanIgnoreReturnValue
    Builder putColumnSizes(Map.Entry<Integer, ? extends Long> entry);

    @CanIgnoreReturnValue
    Builder columnSizes(Map<Integer, ? extends Long> entries);

    @CanIgnoreReturnValue
    Builder putAllColumnSizes(Map<Integer, ? extends Long> entries);

    @CanIgnoreReturnValue
    Builder putValueCounts(int key, long value);

    @CanIgnoreReturnValue
    Builder putValueCounts(Map.Entry<Integer, ? extends Long> entry);

    @CanIgnoreReturnValue
    Builder valueCounts(Map<Integer, ? extends Long> entries);

    @CanIgnoreReturnValue
    Builder putAllValueCounts(Map<Integer, ? extends Long> entries);

    @CanIgnoreReturnValue
    Builder putNullValueCounts(int key, long value);

    @CanIgnoreReturnValue
    Builder putNullValueCounts(Map.Entry<Integer, ? extends Long> entry);

    @CanIgnoreReturnValue
    Builder nullValueCounts(Map<Integer, ? extends Long> entries);

    @CanIgnoreReturnValue
    Builder putAllNullValueCounts(Map<Integer, ? extends Long> entries);

    @CanIgnoreReturnValue
    Builder putNanValueCounts(int key, long value);

    @CanIgnoreReturnValue
    Builder putNanValueCounts(Map.Entry<Integer, ? extends Long> entry);

    @CanIgnoreReturnValue
    Builder nanValueCounts(Map<Integer, ? extends Long> entries);

    @CanIgnoreReturnValue
    Builder putAllNanValueCounts(Map<Integer, ? extends Long> entries);

    @CanIgnoreReturnValue
    Builder putLowerBounds(int key, ByteBuffer value);

    @CanIgnoreReturnValue
    Builder putLowerBounds(Map.Entry<Integer, ? extends ByteBuffer> entry);

    @CanIgnoreReturnValue
    Builder lowerBounds(Map<Integer, ? extends ByteBuffer> entries);

    @CanIgnoreReturnValue
    Builder putAllLowerBounds(Map<Integer, ? extends ByteBuffer> entries);

    @CanIgnoreReturnValue
    Builder putUpperBounds(int key, ByteBuffer value);

    @CanIgnoreReturnValue
    Builder putUpperBounds(Map.Entry<Integer, ? extends ByteBuffer> entry);

    @CanIgnoreReturnValue
    Builder upperBounds(Map<Integer, ? extends ByteBuffer> entries);

    @CanIgnoreReturnValue
    Builder putAllUpperBounds(Map<Integer, ? extends ByteBuffer> entries);

    @CanIgnoreReturnValue
    Builder keyMetadata(@Nullable ByteBuffer keyMetadata);

    @CanIgnoreReturnValue
    Builder addSplitOffsets(long element);

    @CanIgnoreReturnValue
    Builder addSplitOffsets(long... elements);

    @CanIgnoreReturnValue
    Builder splitOffsets(Iterable<Long> elements);

    @CanIgnoreReturnValue
    Builder addAllSplitOffsets(Iterable<Long> elements);

    @CanIgnoreReturnValue
    Builder addEqualityIds(int element);

    @CanIgnoreReturnValue
    Builder addEqualityIds(int... elements);

    @CanIgnoreReturnValue
    Builder equalityIds(Iterable<Integer> elements);

    @CanIgnoreReturnValue
    Builder addAllEqualityIds(Iterable<Integer> elements);

    @CanIgnoreReturnValue
    Builder sortOrderId(int sortOrderId);

    @CanIgnoreReturnValue
    Builder specId(@Nullable Integer specId);

    @CanIgnoreReturnValue
    Builder partition(GenericData.Record partition);

    IcebergDataFile build();
  }
}
