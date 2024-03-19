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
package org.projectnessie.catalog.service.server.spark;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.errorprone.annotations.FormatMethod;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractSparkSmokeTests {

  protected final int httpPort = Integer.getInteger("quarkus.http.test-port");

  private SparkSession spark;

  private String namespace;

  @BeforeAll
  void init(@TempDir Path warehouseDir) {
    SparkConf conf = sparkConf(warehouseDir);
    spark = SparkSession.builder().master("local[2]").config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
  }

  protected SparkConf sparkConf(Path warehouseDir) {
    return new SparkConf()
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.warehouse", warehouseDir.toString())
        .set("spark.sql.warehouse.dir", warehouseDir.toUri().toString())
        .set("spark.ui.enabled", "false")
        .set("spark.testing", "true")
        .set("spark.sql.shuffle.partitions", "4")
        .set(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic");
  }

  @AfterAll
  void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @BeforeEach
  void createNamespace() {
    namespace = "nessie.testing" + System.currentTimeMillis();
    sql("CREATE NAMESPACE %s;", namespace);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void icebergSpecs(int specVersion) {
    String tableName = table("smoke_spec_" + specVersion);

    sql(
        """
         CREATE TABLE %s (
           C_CITYKEY BIGINT, C_NAME STRING, N_NATIONKEY BIGINT, C_COMMENT STRING
         ) USING iceberg
         PARTITIONED BY (bucket(16, N_NATIONKEY))
         TBLPROPERTIES ('format-version' = '%d');
         """,
        tableName, specVersion);

    sql("INSERT INTO %s VALUES (1, 'a', 1, 'comment');", tableName);

    List<Row> rows = sql("SELECT * FROM %s;", tableName).collectAsList();

    assertThat(rows)
        .hasSize(1)
        .extracting(
            row -> List.of(row.getLong(0), row.getString(1), row.getLong(2), row.getString(3)))
        .containsExactly(List.of(1L, "a", 1L, "comment"));
  }

  String table(String name) {
    return String.format("%s.%s", namespace, name);
  }

  @FormatMethod
  Dataset<Row> sql(String sql, Object... args) {
    return spark.sql(String.format(sql, args));
  }
}
