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

import static java.util.Objects.requireNonNull;
import static org.projectnessie.catalog.formats.iceberg.manifest.AvroSerializationContext.dataSerializationContext;
import static org.projectnessie.catalog.formats.iceberg.manifest.AvroSerializationContext.deleteSerializationContext;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary.icebergPartitionFieldSummary;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergStructType.FIELD_ID_PROP;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public abstract class IcebergManifestFileWriter {

  public static Builder builder() {
    return ImmutableIcebergManifestFileWriter.builder();
  }

  public abstract IcebergSpec spec();

  public abstract IcebergSchema schema();

  public abstract IcebergPartitionSpec partitionSpec();

  public abstract long addedSnapshotId();

  public abstract IcebergManifestContent content();

  public abstract long sequenceNumber();

  public abstract long minSequenceNumber();

  public abstract Map<String, String> tableProperties();

  @Nullable
  @jakarta.annotation.Nullable
  public abstract byte[] keyMetadata();

  public interface Builder {

    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder spec(IcebergSpec spec);

    @CanIgnoreReturnValue
    Builder schema(IcebergSchema schema);

    @CanIgnoreReturnValue
    Builder partitionSpec(IcebergPartitionSpec partitionSpec);

    @CanIgnoreReturnValue
    Builder addedSnapshotId(long addedSnapshotId);

    @CanIgnoreReturnValue
    Builder content(IcebergManifestContent content);

    @CanIgnoreReturnValue
    Builder sequenceNumber(long sequenceNumber);

    @CanIgnoreReturnValue
    Builder minSequenceNumber(long minSequenceNumber);

    @CanIgnoreReturnValue
    Builder keyMetadata(byte[] keyMetadata);

    @CanIgnoreReturnValue
    Builder putTableProperty(String key, String value);

    @CanIgnoreReturnValue
    Builder putTableProperty(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder tableProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllTableProperties(Map<String, ? extends String> ent0ries);

    IcebergManifestFileWriter build();
  }

  @Value.Lazy
  Schema writerSchema() {
    AvroTyped<IcebergManifestEntry> avroManifestEntry = spec().avroBundle().schemaManifestEntry();
    return avroManifestEntry.writeSchema(
        AvroReadWriteContext.builder()
            .putSchemaOverride("data_file.partition", partitionSpec().avroSchema(schema(), "r102"))
            .build());
  }

  public interface IcebergManifestFileEntryWriter extends AutoCloseable {
    IcebergManifestFileEntryWriter append(
        IcebergDataFile dataFile,
        IcebergManifestEntryStatus status,
        Long fileSequenceNumber,
        Long sequenceNumber,
        Long snapshotId);

    IcebergManifestFileEntryWriter append(IcebergManifestEntry entry);

    IcebergManifestFile finish() throws IOException;
  }

  @Value.Lazy
  public IcebergManifestFileEntryWriter entryWriter(OutputStream output, String manifestPath) {
    String schemaJson;
    String specJson;
    // TODO String entrySchemaJson;
    try {
      // TODO memoize JSON-serialized schema, externally
      schemaJson = spec().jsonWriter().writeValueAsString(schema());
      // TODO memoize JSON-serialized partition spec, externally
      specJson = spec().jsonWriter().writeValueAsString(partitionSpec().fields());
      // TODO memoize JSON-serialized entry schema, externally
      // TODO entrySchemaJson = spec().jsonWriter().writeValueAsString();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    DataFileWriter<IcebergManifestEntry> entryWriter = buildDataFileWriter();
    entryWriter.setMeta("schema", schemaJson);
    entryWriter.setMeta("partition-spec", specJson);
    entryWriter.setMeta("partition-spec-id", Integer.toString(partitionSpec().specId()));
    entryWriter.setMeta("format-version", Integer.toString(spec().version()));
    // TODO add 'iceberg.schema', which is the Avro schema as an Iceberg schema
    if (spec().version() >= 2) {
      entryWriter.setMeta("content", content().stringValue());
    }

    AvroSerializationContext serializationContext;
    switch (content()) {
      case DATA:
        serializationContext = dataSerializationContext(tableProperties());
        break;
      case DELETES:
        serializationContext = deleteSerializationContext(tableProperties());
        break;
      default:
        throw new IllegalStateException("Unknown manifest content " + content());
    }
    entryWriter.setCodec(serializationContext.codec());

    Schema entryWriteSchema = writerSchema();

    OutputContext outputContext = new OutputContext(output);

    try {
      entryWriter = entryWriter.create(entryWriteSchema, outputContext);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return new FileEntryWriterImpl(
        entryWriter,
        outputContext,
        IcebergManifestFile.builder()
            .sequenceNumber(sequenceNumber())
            .minSequenceNumber(minSequenceNumber())
            .manifestPath(manifestPath)
            //
            .content(content())
            .addedSnapshotId(addedSnapshotId())
            .partitionSpecId(partitionSpec().specId())
            .keyMetadata(keyMetadata()));
  }

  private DataFileWriter<IcebergManifestEntry> buildDataFileWriter() {
    DatumWriter<IcebergManifestEntry> datumWriter =
        new DatumWriter<IcebergManifestEntry>() {
          private Schema writeSchema;
          private final AvroTyped<IcebergManifestEntry> avroManifestEntry =
              spec().avroBundle().schemaManifestEntry();

          @Override
          public void setSchema(Schema schema) {
            this.writeSchema = schema;
          }

          @Override
          public void write(IcebergManifestEntry datum, Encoder out) throws IOException {
            avroManifestEntry.write(out, datum, writeSchema);
          }
        };

    return new DataFileWriter<>(datumWriter);
  }

  private final class FileEntryWriterImpl implements IcebergManifestFileEntryWriter {
    private final DataFileWriter<IcebergManifestEntry> entryWriter;
    private final OutputContext outputContext;
    private final IcebergManifestFile.Builder manifestFile;
    private int deletedDataFilesCount;
    private long deletedRowsCount;
    private int addedDataFilesCount;
    private long addedRowsCount;
    private int existingDataFilesCount;
    private long existingRowsCount;
    private PartitionFieldSummaryBuilder[] summary;

    FileEntryWriterImpl(
        DataFileWriter<IcebergManifestEntry> entryWriter,
        OutputContext outputContext,
        IcebergManifestFile.Builder manifestFile) {
      this.entryWriter = entryWriter;
      this.outputContext = outputContext;
      this.manifestFile = manifestFile;

      List<IcebergPartitionField> partitionFields = partitionSpec().fields();
      summary = new PartitionFieldSummaryBuilder[partitionFields.size()];
      for (int i = 0; i < summary.length; i++) {
        IcebergPartitionField field = partitionFields.get(i);
        summary[i] = new PartitionFieldSummaryBuilder(field.type(schema()));
      }
    }

    @Override
    public void close() throws Exception {
      entryWriter.close();
    }

    @Override
    public IcebergManifestFileEntryWriter append(
        IcebergDataFile dataFile,
        IcebergManifestEntryStatus status,
        Long fileSequenceNumber,
        Long sequenceNumber,
        Long snapshotId) {
      IcebergManifestEntry.Builder entry =
          IcebergManifestEntry.builder()
              .dataFile(dataFile)
              .status(status)
              .snapshotId(snapshotId)
              .fileSequenceNumber(fileSequenceNumber)
              .sequenceNumber(sequenceNumber);
      return append(entry.build());
    }

    @Override
    public IcebergManifestFileEntryWriter append(IcebergManifestEntry entry) {
      try {
        entryWriter.append(entry);

        GenericData.Record partition = entry.dataFile().partition();
        List<Schema.Field> partitionSchemeFields = partition.getSchema().getFields();
        List<IcebergPartitionField> fields = partitionSpec().fields();
        for (int i = 0; i < fields.size(); i++) {
          IcebergPartitionField partitionField = fields.get(i);

          int pos = -1;
          for (Schema.Field field : partitionSchemeFields) {
            String fieldId = field.getProp(FIELD_ID_PROP);
            if (fieldId != null) {
              if (partitionField.fieldId() == Integer.parseInt(fieldId)) {
                pos = field.pos();
                break;
              }
            }
          }
          Object value = pos == -1 ? partition.get(partitionField.name()) : partition.get(pos);
          PartitionFieldSummaryBuilder summaryBuilder = summary[i];
          if (value == null) {
            summaryBuilder.containsNull = true;
          } else {
            Object lowerBound = summaryBuilder.lowerBound;
            if (lowerBound == null) {
              summaryBuilder.lowerBound = value;
            } else if (summaryBuilder.type.compare(lowerBound, value) < 0) {
              summaryBuilder.lowerBound = value;
            }
            Object upperBound = summaryBuilder.upperBound;
            if (upperBound == null) {
              summaryBuilder.upperBound = value;
            } else if (summaryBuilder.type.compare(lowerBound, value) > 0) {
              summaryBuilder.lowerBound = value;
            }
            // TODO partition-summary NaN
          }
        }

        long recordCount = requireNonNull(entry.dataFile()).recordCount();
        switch (entry.status()) {
          case ADDED:
            addedDataFilesCount++;
            addedRowsCount += recordCount;
            break;
          case DELETED:
            deletedDataFilesCount++;
            deletedRowsCount += recordCount;
            break;
          case EXISTING:
            existingDataFilesCount++;
            existingRowsCount += recordCount;
            break;
          default:
            throw new IllegalArgumentException("Unknown manifest-entry status " + entry.status());
        }

        return this;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public IcebergManifestFile finish() throws IOException {
      entryWriter.flush();

      for (PartitionFieldSummaryBuilder partitionFieldSummaryBuilder : summary) {
        manifestFile.addPartitions(partitionFieldSummaryBuilder.asSummary());
      }

      return manifestFile
          .manifestLength(outputContext.bytesWritten())
          //
          .deletedDataFilesCount(deletedDataFilesCount)
          .deletedRowsCount(deletedRowsCount)
          .addedRowsCount(addedRowsCount)
          .addedDataFilesCount(addedDataFilesCount)
          .existingRowsCount(existingRowsCount)
          .existingDataFilesCount(existingDataFilesCount)
          .build();
    }
  }

  private static final class PartitionFieldSummaryBuilder {
    final IcebergType type;
    boolean containsNull;
    Object lowerBound;
    Object upperBound;
    Boolean containsNan;

    PartitionFieldSummaryBuilder(IcebergType type) {
      this.type = type;
    }

    IcebergPartitionFieldSummary asSummary() {
      // TODO serialize lower-bound and upper-bound to byte-buffer
      // TODO "If -0.0 is a value of the partition field, the lower_bound must not be +0.0, and if
      //  +0.0 is a value of the partition field, the upper_bound must not be -0.0."
      // TODO see https://iceberg.apache.org/spec/#binary-single-value-serialization
      Object lower = lowerBound;
      byte[] lowerBoundBytes = lower != null ? type.serializeSingleValue(lower) : null;
      Object upper = upperBound;
      byte[] upperBoundBytes = upper != null ? type.serializeSingleValue(upper) : null;
      return icebergPartitionFieldSummary(
          containsNull, lowerBoundBytes, upperBoundBytes, containsNan);
    }
  }
}
