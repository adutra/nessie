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

import java.nio.file.Path;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ITSparkSmoke {

  static final int HTTP_PORT = Integer.getInteger("quarkus.http.test-port");

  static SparkSession spark;

  @BeforeAll
  static void init(@TempDir Path warehouseDir) {
    SparkConf conf =
        new SparkConf()
            .set(
                "spark.sql.catalog.nessie.uri",
                String.format("http://127.0.0.1:%d/api/v1", HTTP_PORT))
            .set("spark.sql.catalog.nessie.ref", "main")
            .set(
                "spark.sql.catalog.nessie.catalog-impl",
                "org.apache.iceberg.nessie.NessieCatalogIcebergCatalog")
            .set("spark.sql.catalog.nessie.warehouse", "/tmp/nessie-catalog-demo")
            .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
            .set(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .set("spark.ui.enabled", "false")
            .set("spark.testing", "true")
            .set("spark.sql.warehouse.dir", warehouseDir.toUri().toString())
            .set("spark.sql.shuffle.partitions", "4")
            .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog");

    spark = SparkSession.builder().master("local[2]").config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void foo() {
    spark.sql("""
        CREATE NAMESPACE nessie.testing;
        """);
    spark.sql(
        """
         CREATE TABLE nessie.testing.city (
           C_CITYKEY BIGINT, C_NAME STRING, N_NATIONKEY BIGINT, C_COMMENT STRING
         ) USING iceberg PARTITIONED BY (bucket(16, N_NATIONKEY));
         """);
    spark.sql(
        """
         INSERT INTO nessie.testing.city VALUES (1, 'a', 1, 'comment');
         """);

    List<Row> rows =
        spark.sql("""
      SELECT * FROM nessie.testing.city;
      """).collectAsList();

    assertThat(rows)
        .hasSize(1)
        .extracting(
            row -> List.of(row.getLong(0), row.getString(1), row.getLong(2), row.getString(3)))
        .containsExactly(List.of(1L, "a", 1L, "comment"));
  }
}
