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
package org.projectnessie.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@QuarkusIntegrationTest
public class ITIcebergCatalog extends CatalogTests<RESTCatalog> {

  // TODO remove this once there's Iceberg 1.5 (the field's `protected` there)
  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("id", 16).build();

  // TODO remove this once there's Iceberg 1.5 (the field's `protected` there)
  protected static final SortOrder WRITE_ORDER =
      SortOrder.builderFor(SCHEMA).asc(Expressions.bucket("id", 16)).asc("id").build();

  protected static final Namespace NS = Namespace.of("newdb");

  private final List<RESTCatalog> catalogs = new ArrayList<>();

  @Override
  protected RESTCatalog catalog() {
    int catalogServerPort = Integer.getInteger("quarkus.http.port");

    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        "nessie-iceberg-api",
        ImmutableMap.of(
            CatalogProperties.URI,
            String.format("http://127.0.0.1:%d/iceberg/", catalogServerPort)));
    catalogs.add(catalog);
    return catalog;
  }

  @AfterEach
  void cleanup() throws Exception {
    for (RESTCatalog catalog : catalogs) {
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

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  /** Nessie Catalog does not expose previous metadata files - at least not yet. */
  @Override
  public void assertPreviousMetadataFileCount(Table table, int metadataFileCount) {
    TableOperations ops = ((BaseTable) table).operations();
    Assertions.assertThat(ops.current().previousFiles()).isEmpty();
  }

  /**
   * Similar to {@link #testRegisterTable()} but places a table-metadata file in the local file
   * system.
   *
   * <p>Need a separate test case, because {@link #testRegisterTable()} uses the metadata-location
   * returned from the server, which points to the Nessie Catalog URL, and Nessie Catalog
   * table-metadata URLs are treated differently. This test cases exercises the "else" part that
   * registers a table from an external object store.
   */
  @Test
  public void testRegisterTableFromFileSystem(@TempDir Path dir) throws Exception {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    Map<String, String> properties =
        org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.of(
            "user", "someone", "created-at", "2023-01-15T00:00:01");
    Table originalTable =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .create();

    originalTable.newFastAppend().appendFile(FILE_A).commit();

    TableOperations ops = ((BaseTable) originalTable).operations();

    Path metadataFile = dir.resolve("my-metadata.json");
    Files.writeString(metadataFile, TableMetadataParser.toJson(ops.current()));

    catalog.dropTable(TABLE, false /* do not purge */);

    Table registeredTable = catalog.registerTable(TABLE, metadataFile.toString());

    Assertions.assertThat(registeredTable).isNotNull();
    Assertions.assertThat(catalog.tableExists(TABLE)).as("Table must exist").isTrue();
    Assertions.assertThat(registeredTable.properties())
        .as("Props must match")
        .containsAllEntriesOf(properties);
    Assertions.assertThat(registeredTable.schema().asStruct())
        .as("Schema must match")
        .isEqualTo(originalTable.schema().asStruct());
    Assertions.assertThat(registeredTable.specs())
        .as("Specs must match")
        .isEqualTo(originalTable.specs());
    Assertions.assertThat(registeredTable.sortOrders())
        .as("Sort orders must match")
        .isEqualTo(originalTable.sortOrders());
    Assertions.assertThat(registeredTable.currentSnapshot())
        .as("Current snapshot must match")
        .isEqualTo(originalTable.currentSnapshot());
    Assertions.assertThat(registeredTable.snapshots())
        .as("Snapshots must match")
        .isEqualTo(originalTable.snapshots());
    Assertions.assertThat(registeredTable.history())
        .as("History must match")
        .isEqualTo(originalTable.history());

    Assertions.assertThat(catalog.loadTable(TABLE)).isNotNull();
    Assertions.assertThat(catalog.dropTable(TABLE)).isTrue();
    Assertions.assertThat(catalog.tableExists(TABLE)).isFalse();
  }

  // TODO the following tests have been copied from Iceberg's `TestRESTCatalog`. Those should
  //  ideally live in `CatalogTests`.

  @Test
  public void diffAgainstSingleTable() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace namespace = Namespace.of("namespace");
    catalog.createNamespace(namespace);
    TableIdentifier identifier = TableIdentifier.of(namespace, "multipleDiffsAgainstSingleTable");

    Table table = catalog.buildTable(identifier, SCHEMA).create();
    Transaction transaction = table.newTransaction();

    UpdateSchema updateSchema =
        transaction.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdatePartitionSpec updateSpec =
        transaction.updateSpec().addField("shard", Expressions.bucket("id", 16));
    PartitionSpec expectedSpec = updateSpec.apply();
    updateSpec.commit();

    TableCommit tableCommit =
        TableCommit.create(
            identifier,
            ((BaseTransaction) transaction).startMetadata(),
            ((BaseTransaction) transaction).currentMetadata());

    catalog.commitTransaction(tableCommit);

    Table loaded = catalog.loadTable(identifier);
    assertThat(loaded.schema().asStruct()).isEqualTo(expectedSchema.asStruct());
    assertThat(loaded.spec().fields()).isEqualTo(expectedSpec.fields());
  }

  @Test
  public void multipleDiffsAgainstMultipleTables() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace namespace = Namespace.of("multiDiffNamespace");
    catalog.createNamespace(namespace);
    TableIdentifier identifier1 = TableIdentifier.of(namespace, "multiDiffTable1");
    TableIdentifier identifier2 = TableIdentifier.of(namespace, "multiDiffTable2");

    Table table1 = catalog.buildTable(identifier1, SCHEMA).create();
    Table table2 = catalog.buildTable(identifier2, SCHEMA).create();
    Transaction t1Transaction = table1.newTransaction();
    Transaction t2Transaction = table2.newTransaction();

    UpdateSchema updateSchema =
        t1Transaction.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdateSchema updateSchema2 =
        t2Transaction.updateSchema().addColumn("new_col2", Types.LongType.get());
    Schema expectedSchema2 = updateSchema2.apply();
    updateSchema2.commit();

    TableCommit tableCommit1 =
        TableCommit.create(
            identifier1,
            ((BaseTransaction) t1Transaction).startMetadata(),
            ((BaseTransaction) t1Transaction).currentMetadata());

    TableCommit tableCommit2 =
        TableCommit.create(
            identifier2,
            ((BaseTransaction) t2Transaction).startMetadata(),
            ((BaseTransaction) t2Transaction).currentMetadata());

    catalog.commitTransaction(tableCommit1, tableCommit2);

    assertThat(catalog.loadTable(identifier1).schema().asStruct())
        .isEqualTo(expectedSchema.asStruct());

    assertThat(catalog.loadTable(identifier2).schema().asStruct())
        .isEqualTo(expectedSchema2.asStruct());
  }

  @Test
  public void multipleDiffsAgainstMultipleTablesLastFails() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace namespace = Namespace.of("multiDiffNamespace");
    catalog.createNamespace(namespace);
    TableIdentifier identifier1 = TableIdentifier.of(namespace, "multiDiffTable1");
    TableIdentifier identifier2 = TableIdentifier.of(namespace, "multiDiffTable2");

    catalog.createTable(identifier1, SCHEMA);
    catalog.createTable(identifier2, SCHEMA);

    Table table1 = catalog.loadTable(identifier1);
    Table table2 = catalog.loadTable(identifier2);
    Schema originalSchemaOne = table1.schema();

    Transaction t1Transaction = catalog.loadTable(identifier1).newTransaction();
    t1Transaction.updateSchema().addColumn("new_col1", Types.LongType.get()).commit();

    Transaction t2Transaction = catalog.loadTable(identifier2).newTransaction();
    t2Transaction.updateSchema().renameColumn("data", "new-column").commit();

    // delete the colum that is being renamed in the above TX to cause a conflict
    table2
        .updateSchema()
        .addColumn("another", Types.LongType.get())
        .addColumn("more", Types.LongType.get())
        .commit();
    Schema updatedSchemaTwo = table2.schema();

    TableCommit tableCommit1 =
        TableCommit.create(
            identifier1,
            ((BaseTransaction) t1Transaction).startMetadata(),
            ((BaseTransaction) t1Transaction).currentMetadata());

    TableCommit tableCommit2 =
        TableCommit.create(
            identifier2,
            ((BaseTransaction) t2Transaction).startMetadata(),
            ((BaseTransaction) t2Transaction).currentMetadata());

    assertThatThrownBy(() -> catalog.commitTransaction(tableCommit1, tableCommit2))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(
            "Requirement failed: last assigned field id changed: expected 4 != 2");

    Schema schema1 = catalog.loadTable(identifier1).schema();
    assertThat(schema1.asStruct()).isEqualTo(originalSchemaOne.asStruct());

    Schema schema2 = catalog.loadTable(identifier2).schema();
    assertThat(schema2.asStruct()).isEqualTo(updatedSchemaTwo.asStruct());
    assertThat(schema2.findField("data")).isNotNull();
    assertThat(schema2.findField("another")).isNotNull();
    assertThat(schema2.findField("more")).isNotNull();
    assertThat(schema2.findField("new-column")).isNull();
    assertThat(schema2.columns()).hasSize(4);
  }

  // TODO port this to Iceberg

  protected boolean exposesHistory() {
    return false;
  }

  @Test
  @Disabled("Port to Iceberg")
  @Override
  public void testMetadataFileLocationsRemovalAfterCommit() {
    assumeThat(exposesHistory()).isTrue();

    RESTCatalog catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    table.updateSchema().addColumn("a", Types.LongType.get()).commit();
    table.updateSchema().addColumn("b", Types.LongType.get()).commit();
    table.updateSchema().addColumn("c", Types.LongType.get()).commit();

    Set<String> metadataFileLocations = ReachableFileUtil.metadataFileLocations(table, false);
    Assertions.assertThat(metadataFileLocations).hasSize(4);

    int maxPreviousVersionsToKeep = 2;
    table
        .updateProperties()
        .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
        .set(
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
            Integer.toString(maxPreviousVersionsToKeep))
        .commit();

    metadataFileLocations = ReachableFileUtil.metadataFileLocations(table, false);
    Assertions.assertThat(metadataFileLocations).hasSize(maxPreviousVersionsToKeep + 1);

    // for each new commit, the amount of metadata files should stay the same and old files should
    // be deleted
    for (int i = 1; i <= 5; i++) {
      table.updateSchema().addColumn("d" + i, Types.LongType.get()).commit();
      metadataFileLocations = ReachableFileUtil.metadataFileLocations(table, false);
      Assertions.assertThat(metadataFileLocations).hasSize(maxPreviousVersionsToKeep + 1);
    }

    maxPreviousVersionsToKeep = 4;
    table
        .updateProperties()
        .set(
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
            Integer.toString(maxPreviousVersionsToKeep))
        .commit();

    // for each new commit, the amount of metadata files should stay the same and old files should
    // be deleted
    for (int i = 1; i <= 10; i++) {
      table.updateSchema().addColumn("e" + i, Types.LongType.get()).commit();
      metadataFileLocations = ReachableFileUtil.metadataFileLocations(table, false);
      Assertions.assertThat(metadataFileLocations).hasSize(maxPreviousVersionsToKeep + 1);
    }
  }
}
