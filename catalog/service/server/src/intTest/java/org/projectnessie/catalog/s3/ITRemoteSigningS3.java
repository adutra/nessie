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
package org.projectnessie.catalog.s3;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.nessie.NessieCatalogIcebergCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.minio.MinioContainer;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

@QuarkusTestResource(
    restrictToAnnotatedClass = true,
    value = MinioTestResourceLifecycleManager.class)
@QuarkusIntegrationTest
public class ITRemoteSigningS3 {

  private MinioContainer minio; // Injected by MinioTestResourceLifecycleManager
  private NessieCatalogIcebergCatalog catalog;

  @BeforeEach
  void createCatalog() {
    int catalogServerPort = Integer.getInteger("quarkus.http.port");
    catalog = new NessieCatalogIcebergCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        "test-signing",
        ImmutableMap.<String, String>builder()
            .put("uri", format("http://127.0.0.1:%d/api/v2", catalogServerPort))
            .put("client-api-version", "2")
            .put("warehouse", format("s3://%s", minio.bucket()))
            // TODO: investigate whether this can be set automatically
            .put("s3.endpoint", minio.s3endpoint())
            .put("s3.remote-signing-enabled", "true")
            .put("client.region", MinioTestResourceLifecycleManager.TEST_REGION)
            .put("send-updates-to-server", "false")
            .build());
  }

  @AfterEach
  void closeCatalog() throws IOException {
    if (catalog != null) {
      catalog.close();
      catalog = null;
    }
  }

  @Test
  void testSignS3() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    Table table = catalog.createTable(TableIdentifier.of("test1"), schema);

    ListObjectsResponse objects =
        minio.s3Client().listObjects(ListObjectsRequest.builder().bucket(minio.bucket()).build());

    assertThat(objects.isTruncated()).isFalse();
    assertThat(objects.contents())
        .map(o -> "s3://" + minio.bucket() + "/" + o.key())
        .anySatisfy(
            l -> {
              assertThat(l).startsWith(table.location() + "/metadata/00000-");
              assertThat(l).endsWith(".metadata.json");
            });

    table.newAppend().appendFile(dataFile(table)).commit();

    objects =
        minio.s3Client().listObjects(ListObjectsRequest.builder().bucket(minio.bucket()).build());

    assertThat(objects.isTruncated()).isFalse();
    assertThat(objects.contents())
        .map(o -> "s3://" + minio.bucket() + "/" + o.key())
        .anySatisfy(
            l -> {
              assertThat(l).startsWith(table.location() + "/metadata/00000-");
              assertThat(l).endsWith(".metadata.json");
            })
        .anySatisfy(
            l -> {
              assertThat(l).startsWith(table.location() + "/metadata/");
              assertThat(l).endsWith("-m0.avro");
            })
        .anySatisfy(
            l -> {
              assertThat(l).startsWith(table.location() + "/metadata/snap-");
              assertThat(l).endsWith(".avro");
            });
  }

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "value", Types.StringType.get()));

  private DataFile dataFile(Table table) {
    return DataFiles.builder(table.spec())
        .withFormat(FileFormat.PARQUET)
        .withInputFile(Files.localInput("data-1.parquet"))
        .withRecordCount(0)
        .withPartition(partitionKey(table, 1))
        .build();
  }

  private PartitionKey partitionKey(Table table, long id) {
    GenericRecord key = GenericRecord.create(SCHEMA);
    key.set(1, id);
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    partitionKey.partition(key);
    return partitionKey;
  }
}
