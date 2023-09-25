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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public abstract class IcebergManifestListWriter {

  public static Builder builder() {
    return ImmutableIcebergManifestListWriter.builder();
  }

  public abstract IcebergSpec spec();

  public abstract IcebergSchema schema();

  public abstract IcebergPartitionSpec partitionSpec();

  public abstract Map<String, String> tableProperties();

  public abstract long snapshotId();

  public abstract long parentSnapshotId();

  @Value.Default
  public long sequenceNumber() {
    return 0L;
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
    Builder snapshotId(long snapshotId);

    @CanIgnoreReturnValue
    Builder parentSnapshotId(long parentSnapshotId);

    @CanIgnoreReturnValue
    Builder sequenceNumber(long sequenceNumber);

    @CanIgnoreReturnValue
    Builder putTableProperties(String key, String value);

    @CanIgnoreReturnValue
    Builder putTableProperties(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder tableProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllTableProperties(Map<String, ? extends String> ent0ries);

    IcebergManifestListWriter build();
  }

  public interface IcebergManifestListEntryWriter extends AutoCloseable {
    IcebergManifestListEntryWriter append(IcebergManifestFile manifestFile);
  }

  @Value.Lazy
  Schema writerSchema() {
    AvroTyped<IcebergManifestFile> avroManifestEntry = spec().avroBundle().schemaManifestFile();

    AvroReadWriteContext avroReadWriteContext = null;
    // AvroReadWriteContext avroReadWriteContext = AvroReadWriteContext.builder()
    //  .putSchemaOverride("data_file.partition", partitionSpec().avroSchema(schema(), "r102"))
    //  .build();

    return avroManifestEntry.writeSchema(avroReadWriteContext);
  }

  @Value.Lazy
  public IcebergManifestListEntryWriter entryWriter(OutputStream output) {
    DataFileWriter<IcebergManifestFile> entryWriter = buildDataFileWriter();
    entryWriter.setMeta("format-version", Integer.toString(spec().version()));
    entryWriter.setMeta("snapshot-id", String.valueOf(snapshotId()));
    entryWriter.setMeta("parent-snapshot-id", String.valueOf(parentSnapshotId()));
    if (spec().version() >= 2) {
      entryWriter.setMeta("sequence-number", String.valueOf(sequenceNumber()));
    }

    AvroSerializationContext serializationContext = dataSerializationContext(tableProperties());
    entryWriter.setCodec(serializationContext.codec());

    Schema entryWriteSchema = writerSchema();

    OutputContext outputContext = new OutputContext(output);

    try {
      entryWriter = entryWriter.create(entryWriteSchema, outputContext);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return new ManifestListWriterImpl(entryWriter);
  }

  private DataFileWriter<IcebergManifestFile> buildDataFileWriter() {
    DatumWriter<IcebergManifestFile> datumWriter =
        new DatumWriter<IcebergManifestFile>() {
          private Schema writeSchema;
          private final AvroTyped<IcebergManifestFile> avroManifestFile =
              spec().avroBundle().schemaManifestFile();

          @Override
          public void setSchema(Schema schema) {
            this.writeSchema = schema;
          }

          @Override
          public void write(IcebergManifestFile datum, Encoder out) throws IOException {
            avroManifestFile.write(out, datum, writeSchema);
          }
        };

    return new DataFileWriter<>(datumWriter);
  }

  private static final class ManifestListWriterImpl implements IcebergManifestListEntryWriter {
    private final DataFileWriter<IcebergManifestFile> entryWriter;

    ManifestListWriterImpl(DataFileWriter<IcebergManifestFile> entryWriter) {
      this.entryWriter = entryWriter;
    }

    @Override
    public void close() throws Exception {
      entryWriter.close();
    }

    @Override
    public IcebergManifestListEntryWriter append(IcebergManifestFile entry) {
      try {
        entryWriter.append(entry);

        return this;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
