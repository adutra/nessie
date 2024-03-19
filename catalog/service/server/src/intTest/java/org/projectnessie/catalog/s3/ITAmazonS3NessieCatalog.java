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

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.nessie.NessieCatalogIcebergCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.minio.MinioContainer;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

@QuarkusTestResource(
    restrictToAnnotatedClass = true,
    value = MinioTestResourceLifecycleManager.class)
@QuarkusIntegrationTest
@TestProfile(AmazonCloudProfile.class)
public class ITAmazonS3NessieCatalog
//  extends CatalogTests<NessieCatalogIcebergCatalog>
{

  @SuppressWarnings("unused")
  // Injected by MinioTestResourceLifecycleManager
  private MinioContainer minio;

  private final List<NessieCatalogIcebergCatalog> catalogs = new ArrayList<>();
  private boolean sendUpdatesToServer;

  //  @Override
  protected NessieCatalogIcebergCatalog catalog() {
    int catalogServerPort = Integer.getInteger("quarkus.http.port");
    NessieCatalogIcebergCatalog catalog = new NessieCatalogIcebergCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        "test-signing",
        Map.of(
            CatalogProperties.URI,
            String.format("http://127.0.0.1:%d/api/v2", catalogServerPort),
            "client-api-version",
            "2",
            CatalogProperties.WAREHOUSE_LOCATION,
            minio.s3BucketUri("").toString(),
            S3FileIOProperties.ENDPOINT,
            minio.s3endpoint(),
            AwsClientProperties.CLIENT_REGION,
            MinioTestResourceLifecycleManager.TEST_REGION,
            "send-updates-to-server",
            Boolean.toString(sendUpdatesToServer)));
    catalogs.add(catalog);
    return catalog;
  }

  @AfterEach
  void cleanup() throws Exception {
    for (NessieCatalogIcebergCatalog catalog : catalogs) {
      catalog.close();
    }

    int catalogServerPort = Integer.getInteger("quarkus.http.port");

    try (NessieApiV2 api =
        NessieClientBuilder.createClientBuilderFromSystemSettings()
            .withUri(String.format("http://127.0.0.1:%d/api/v2/", catalogServerPort))
            .build(NessieApiV2.class)) {
      Reference main = null;
      for (Reference reference : api.getAllReferences().stream().toList()) {
        if (reference.getName().equals("main")) {
          main = reference;
        } else {
          api.deleteReference().reference(reference).delete();
        }
      }
      api.assignReference()
          .reference(main)
          .assignTo(Branch.of("main", ObjId.EMPTY_OBJ_ID.toString()))
          .assign();
    }
  }

  //  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  //  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  //  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  //  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  /** Nessie Catalog does not expose previous metadata files - at least not yet. */
  //  @Override
  public void assertPreviousMetadataFileCount(Table table, int metadataFileCount) {
    TableOperations ops = ((BaseTable) table).operations();
    Assertions.assertThat(ops.current().previousFiles()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSignS3(boolean sendUpdatesToServer) {

    this.sendUpdatesToServer = sendUpdatesToServer;

    @SuppressWarnings("resource")
    NessieCatalogIcebergCatalog catalog = catalog();

    Table table = catalog.createTable(TableIdentifier.of("test1"), SCHEMA);

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
