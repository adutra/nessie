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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ReplaceViewVersion;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.iceberg.view.ViewHistoryEntry;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewProperties;
import org.apache.iceberg.view.ViewUtil;
import org.apache.iceberg.view.ViewVersion;
import org.assertj.core.api.RecursiveComparisonAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@QuarkusIntegrationTest
public class ITIcebergViewsCatalog extends ViewCatalogTests<RESTCatalog> {
  private final List<RESTCatalog> catalogs = new ArrayList<>();

  protected RESTCatalog currentCatalog;

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
    currentCatalog = catalog;
    return catalog;
  }

  @SuppressWarnings("resource")
  @Override
  protected Catalog tableCatalog() {
    if (currentCatalog == null) {
      catalog();
    }
    return currentCatalog;
  }

  @AfterEach
  void cleanup() throws Exception {
    currentCatalog = null;

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
  protected boolean supportsServerSideRetry() {
    return true;
  }

  // TODO port the following to Iceberg

  protected static final Schema OTHER_SCHEMA =
      new Schema(7, required(1, "some_id", Types.IntegerType.get()));

  protected boolean exposesHistory() {
    return false;
  }

  protected boolean physicalMetadataLocation() {
    return false;
  }

  @Test
  public void concurrentReplaceViewVersion() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("trino", "select * from ns.tbl")
            .withProperty(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED, "true")
            .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    ReplaceViewVersion replaceViewVersionOne =
        view.replaceVersion()
            .withQuery("trino", "select count(id) from ns.tbl")
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace());

    ReplaceViewVersion replaceViewVersionTwo =
        view.replaceVersion()
            .withQuery("spark", "select count(some_id) from ns.tbl")
            .withSchema(OTHER_SCHEMA)
            .withDefaultNamespace(identifier.namespace());

    // simulate a concurrent replace of the view version
    ViewOperations viewOps = ((BaseView) view).operations();
    ViewMetadata current = viewOps.current();

    ViewMetadata trinoUpdate;
    ViewMetadata sparkUpdate;
    try {
      Method m = replaceViewVersionOne.getClass().getDeclaredMethod("internalApply");
      m.setAccessible(true);
      trinoUpdate = (ViewMetadata) m.invoke(replaceViewVersionTwo);
      sparkUpdate = (ViewMetadata) m.invoke(replaceViewVersionOne);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    viewOps.commit(current, trinoUpdate);

    if (supportsServerSideRetry()) {
      // retry should succeed and the changes should be applied
      viewOps.commit(current, sparkUpdate);

      View updatedView = catalog().loadView(identifier);
      ViewVersion viewVersion = updatedView.currentVersion();
      assertThat(viewVersion.versionId()).isEqualTo(3);
      if (exposesHistory()) {
        assertThat(updatedView.versions()).hasSize(3);

        assertThat(updatedView.version(1))
            .isEqualTo(
                ImmutableViewVersion.builder()
                    .timestampMillis(updatedView.version(1).timestampMillis())
                    .versionId(1)
                    .schemaId(0)
                    .summary(updatedView.version(1).summary())
                    .defaultNamespace(identifier.namespace())
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .sql("select * from ns.tbl")
                            .dialect("trino")
                            .build())
                    .build());

        assertThat(updatedView.version(2))
            .isEqualTo(
                ImmutableViewVersion.builder()
                    .timestampMillis(updatedView.version(2).timestampMillis())
                    .versionId(2)
                    .schemaId(1)
                    .summary(updatedView.version(2).summary())
                    .defaultNamespace(identifier.namespace())
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .sql("select count(some_id) from ns.tbl")
                            .dialect("spark")
                            .build())
                    .build());
      } else {
        assertThat(updatedView.versions()).hasSize(1);
      }
      assertThat(updatedView.version(3))
          .isEqualTo(
              ImmutableViewVersion.builder()
                  .timestampMillis(updatedView.version(3).timestampMillis())
                  .versionId(3)
                  .schemaId(0)
                  .summary(updatedView.version(3).summary())
                  .defaultNamespace(identifier.namespace())
                  .addRepresentations(
                      ImmutableSQLViewRepresentation.builder()
                          .sql("select count(id) from ns.tbl")
                          .dialect("trino")
                          .build())
                  .build());
    } else {
      assertThatThrownBy(() -> viewOps.commit(current, sparkUpdate))
          .isInstanceOf(CommitFailedException.class)
          .hasMessageContaining("Cannot commit");

      View updatedView = catalog().loadView(identifier);
      ViewVersion viewVersion = updatedView.currentVersion();
      assertThat(viewVersion.versionId()).isEqualTo(2);
      assertThat(updatedView.versions()).hasSize(2);
      assertThat(updatedView.version(1))
          .isEqualTo(
              ImmutableViewVersion.builder()
                  .timestampMillis(updatedView.version(1).timestampMillis())
                  .versionId(1)
                  .schemaId(0)
                  .summary(updatedView.version(1).summary())
                  .defaultNamespace(identifier.namespace())
                  .addRepresentations(
                      ImmutableSQLViewRepresentation.builder()
                          .sql("select * from ns.tbl")
                          .dialect("trino")
                          .build())
                  .build());

      assertThat(updatedView.version(2))
          .isEqualTo(
              ImmutableViewVersion.builder()
                  .timestampMillis(updatedView.version(2).timestampMillis())
                  .versionId(2)
                  .schemaId(1)
                  .summary(updatedView.version(2).summary())
                  .defaultNamespace(identifier.namespace())
                  .addRepresentations(
                      ImmutableSQLViewRepresentation.builder()
                          .sql("select count(some_id) from ns.tbl")
                          .dialect("spark")
                          .build())
                  .build());
    }
  }

  @Test
  public void replaceViewVersion() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    SQLViewRepresentation spark =
        ImmutableSQLViewRepresentation.builder()
            .dialect("spark")
            .sql("select * from ns.tbl")
            .build();

    SQLViewRepresentation trino =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from ns.tbl")
            .dialect("trino")
            .build();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery(trino.dialect(), trino.sql())
            .withQuery(spark.dialect(), spark.sql())
            .withProperty(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED, "true")
            .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    ViewVersion viewVersion = view.currentVersion();
    assertThat(viewVersion.representations()).hasSize(2).containsExactly(trino, spark);

    // uses a different schema and view representation
    view.replaceVersion()
        .withSchema(OTHER_SCHEMA)
        .withQuery(trino.dialect(), trino.sql())
        .withDefaultCatalog("default")
        .withDefaultNamespace(identifier.namespace())
        .commit();

    // history and view versions should reflect the changes
    View updatedView = catalog().loadView(identifier);
    if (exposesHistory()) {
      assertThat(updatedView.history())
          .hasSize(2)
          .element(0)
          .extracting(ViewHistoryEntry::versionId)
          .isEqualTo(viewVersion.versionId());
      assertThat(updatedView.history())
          .element(1)
          .extracting(ViewHistoryEntry::versionId)
          .isEqualTo(updatedView.currentVersion().versionId());
      assertThat(updatedView.schemas()).hasSize(2).containsKey(0).containsKey(1);
      assertThat(updatedView.versions())
          .hasSize(2)
          .containsExactly(viewVersion, updatedView.currentVersion());
    } else {
      assertThat(updatedView.history())
          .hasSize(1)
          .element(0)
          .extracting(ViewHistoryEntry::versionId)
          .isEqualTo(updatedView.currentVersion().versionId());
      assertThat(updatedView.schemas()).hasSize(2).containsKey(0).containsKey(1);
      assertThat(updatedView.versions()).hasSize(1).containsExactly(updatedView.currentVersion());
    }

    ViewVersion updatedViewVersion = updatedView.currentVersion();
    assertThat(updatedViewVersion).isNotNull();
    assertThat(updatedViewVersion.versionId()).isEqualTo(viewVersion.versionId() + 1);
    assertThat(updatedViewVersion.operation()).isEqualTo("replace");
    assertThat(updatedViewVersion.representations()).hasSize(1).containsExactly(trino);
    assertThat(updatedViewVersion.schemaId()).isEqualTo(1);
    assertThat(updatedViewVersion.defaultCatalog()).isEqualTo("default");
    assertThat(updatedViewVersion.defaultNamespace()).isEqualTo(identifier.namespace());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @ParameterizedTest(name = ".createOrReplace() = {arguments}")
  @ValueSource(booleans = {false, true})
  public void createOrReplaceView(boolean useCreateOrReplace) {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    ViewBuilder viewBuilder =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("spark", "select * from ns.tbl")
            .withProperty("prop1", "val1")
            .withProperty("prop2", "val2");
    View view = useCreateOrReplace ? viewBuilder.createOrReplace() : viewBuilder.create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    assertThat(((BaseView) view).operations().current().metadataFileLocation()).isNotNull();

    ViewVersion viewVersion = view.currentVersion();
    assertThat(viewVersion.representations())
        .containsExactly(
            ImmutableSQLViewRepresentation.builder()
                .sql("select * from ns.tbl")
                .dialect("spark")
                .build());

    viewBuilder =
        catalog()
            .buildView(identifier)
            .withSchema(OTHER_SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("trino", "select count(*) from ns.tbl")
            .withProperty("replacedProp1", "val1")
            .withProperty("replacedProp2", "val2")
            .withProperty(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED, "true");
    View replacedView = useCreateOrReplace ? viewBuilder.createOrReplace() : viewBuilder.replace();

    // validate replaced view settings
    assertThat(replacedView.name()).isEqualTo(ViewUtil.fullViewName(catalog().name(), identifier));
    assertThat(((BaseView) replacedView).operations().current().metadataFileLocation()).isNotNull();
    assertThat(replacedView.properties())
        .containsEntry("prop1", "val1")
        .containsEntry("prop2", "val2")
        .containsEntry("replacedProp1", "val1")
        .containsEntry("replacedProp2", "val2");
    if (exposesHistory()) {
      assertThat(replacedView.history())
          .hasSize(2)
          .first()
          .extracting(ViewHistoryEntry::versionId)
          .isEqualTo(1);
      assertThat(replacedView.history())
          .element(1)
          .extracting(ViewHistoryEntry::versionId)
          .isEqualTo(2);
    } else {
      assertThat(replacedView.history())
          .hasSize(1)
          .first()
          .extracting(ViewHistoryEntry::versionId)
          .isEqualTo(2);
      assertThat(replacedView.history())
          .first()
          .extracting(ViewHistoryEntry::versionId)
          .isEqualTo(2);
    }

    assertThat(replacedView.schema().schemaId()).isEqualTo(1);
    assertThat(replacedView.schema().asStruct()).isEqualTo(OTHER_SCHEMA.asStruct());
    assertThat(replacedView.schemas()).hasSize(2).containsKey(0).containsKey(1);

    ViewVersion replacedViewVersion = replacedView.currentVersion();
    if (exposesHistory()) {
      assertThat(replacedView.versions())
          .hasSize(2)
          .containsExactly(viewVersion, replacedViewVersion);
    } else {
      assertThat(replacedView.versions()).hasSize(1).containsExactly(replacedViewVersion);
    }
    assertThat(replacedViewVersion).isNotNull();
    assertThat(replacedViewVersion.versionId()).isEqualTo(2);
    assertThat(replacedViewVersion.schemaId()).isEqualTo(1);
    assertThat(replacedViewVersion.operation()).isEqualTo("replace");
    assertThat(replacedViewVersion.representations())
        .containsExactly(
            ImmutableSQLViewRepresentation.builder()
                .sql("select count(*) from ns.tbl")
                .dialect("trino")
                .build());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void replaceViewVersionByUpdatingSQLForDialect() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    SQLViewRepresentation spark =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from ns.tbl")
            .dialect("spark")
            .build();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery(spark.dialect(), spark.sql())
            .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    ViewVersion viewVersion = view.currentVersion();
    assertThat(viewVersion.representations()).hasSize(1).containsExactly(spark);

    SQLViewRepresentation updatedSpark =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from ns.updated_tbl")
            .dialect("spark")
            .build();

    // only update the SQL for spark
    view.replaceVersion()
        .withSchema(SCHEMA)
        .withDefaultNamespace(identifier.namespace())
        .withQuery(updatedSpark.dialect(), updatedSpark.sql())
        .commit();

    // history and view versions should reflect the changes
    View updatedView = catalog().loadView(identifier);
    if (exposesHistory()) {
      assertThat(updatedView.history())
          .hasSize(2)
          .element(0)
          .extracting(ViewHistoryEntry::versionId)
          .isEqualTo(viewVersion.versionId());
      assertThat(updatedView.history())
          .element(1)
          .extracting(ViewHistoryEntry::versionId)
          .isEqualTo(updatedView.currentVersion().versionId());
      assertThat(updatedView.versions())
          .hasSize(2)
          .containsExactly(viewVersion, updatedView.currentVersion());
    } else {
      assertThat(updatedView.history())
          .hasSize(1)
          .element(0)
          .extracting(ViewHistoryEntry::versionId)
          .isEqualTo(updatedView.currentVersion().versionId());
      assertThat(updatedView.versions()).hasSize(1).containsExactly(updatedView.currentVersion());
    }

    // updated view should have the new SQL
    assertThat(updatedView.currentVersion().representations())
        .hasSize(1)
        .containsExactly(updatedSpark);

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void renameView() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("ns", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
    }

    assertThat(catalog().viewExists(from)).as("View should not exist").isFalse();

    View view =
        catalog()
            .buildView(from)
            .withSchema(SCHEMA)
            .withDefaultNamespace(from.namespace())
            .withQuery("spark", "select * from ns.tbl")
            .create();

    assertThat(catalog().viewExists(from)).as("View should exist").isTrue();

    ViewMetadata original = ((BaseView) view).operations().current();
    assertThat(original.metadataFileLocation()).isNotNull();

    catalog().renameView(from, to);

    assertThat(catalog().viewExists(from)).as("View should not exist with old name").isFalse();
    assertThat(catalog().viewExists(to)).as("View should exist with new name").isTrue();

    // ensure view metadata didn't change after renaming
    View renamed = catalog().loadView(to);
    RecursiveComparisonAssert<?> recursiveAssert =
        assertThat(((BaseView) renamed).operations().current())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Schema.class);
    if (!physicalMetadataLocation()) {
      recursiveAssert =
          recursiveAssert
              .ignoringFields("metadataFileLocation")
              .ignoringFieldsMatchingRegexes("properties[.]nessie[.].*");
    }
    recursiveAssert.isEqualTo(original);

    assertThat(catalog().dropView(from)).isFalse();
    assertThat(catalog().dropView(to)).isTrue();
    assertThat(catalog().viewExists(to)).as("View should not exist").isFalse();
  }

  @Test
  public void renameViewUsingDifferentNamespace() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("other_ns", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
      catalog().createNamespace(to.namespace());
    }

    assertThat(catalog().viewExists(from)).as("View should not exist").isFalse();

    View view =
        catalog()
            .buildView(from)
            .withSchema(SCHEMA)
            .withDefaultNamespace(from.namespace())
            .withQuery("spark", "select * from ns.tbl")
            .create();

    assertThat(catalog().viewExists(from)).as("View should exist").isTrue();

    ViewMetadata original = ((BaseView) view).operations().current();

    catalog().renameView(from, to);

    assertThat(catalog().viewExists(from)).as("View should not exist with old name").isFalse();
    assertThat(catalog().viewExists(to)).as("View should exist with new name").isTrue();

    // ensure view metadata didn't change after renaming
    View renamed = catalog().loadView(to);
    RecursiveComparisonAssert<?> recursiveAssert =
        assertThat(((BaseView) renamed).operations().current())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Schema.class);
    if (!physicalMetadataLocation()) {
      recursiveAssert =
          recursiveAssert
              .ignoringFields("metadataFileLocation")
              .ignoringFieldsMatchingRegexes("properties[.]nessie[.].*");
    }
    recursiveAssert.isEqualTo(original);

    assertThat(catalog().dropView(from)).isFalse();
    assertThat(catalog().dropView(to)).isTrue();
    assertThat(catalog().viewExists(to)).as("View should not exist").isFalse();
  }
}
