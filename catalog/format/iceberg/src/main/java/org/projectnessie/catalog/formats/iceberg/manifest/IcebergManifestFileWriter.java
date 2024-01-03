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

import static org.projectnessie.catalog.formats.iceberg.manifest.AvroSerializationContext.dataSerializationContext;
import static org.projectnessie.catalog.formats.iceberg.manifest.AvroSerializationContext.deleteSerializationContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.nessie.immutables.NessieImmutable;

// TODO make the writer reusable, so it can memoize Avro schemas.
//  (see org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.avroSchema)
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

  public abstract OutputStream output();

  public abstract String manifestPath();

  @Value.Default
  public boolean closeOutput() {
    return false;
  }

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
    Builder output(OutputStream output);

    @CanIgnoreReturnValue
    Builder closeOutput(boolean closeOutput);

    @CanIgnoreReturnValue
    Builder manifestPath(String manifestPath);

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
  public IcebergManifestFileEntryWriter entryWriter() {
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
    serializationContext.applyToDataFileWriter(entryWriter);

    Schema entryWriteSchema = writerSchema();

    OutputContext outputContext = new OutputContext(output(), closeOutput());

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
            .manifestPath(manifestPath())
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
    private final IcebergColumnStatsCollector statsCollector;

    FileEntryWriterImpl(
        DataFileWriter<IcebergManifestEntry> entryWriter,
        OutputContext outputContext,
        IcebergManifestFile.Builder manifestFile) {
      this.entryWriter = entryWriter;
      this.outputContext = outputContext;
      this.manifestFile = manifestFile;
      this.statsCollector = new IcebergColumnStatsCollector(schema(), partitionSpec());
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

        statsCollector.addManifestEntry(entry);

        return this;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public IcebergManifestFile finish() throws IOException {
      entryWriter.flush();

      statsCollector.addToManifestFileBuilder(manifestFile);

      return manifestFile.manifestLength(outputContext.bytesWritten()).build();
    }
  }
}
