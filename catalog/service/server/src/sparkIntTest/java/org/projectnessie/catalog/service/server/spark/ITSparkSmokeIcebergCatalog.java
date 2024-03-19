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

import java.nio.file.Path;
import org.apache.spark.SparkConf;

public class ITSparkSmokeIcebergCatalog extends AbstractSparkSmokeTests {

  @Override
  protected SparkConf sparkConf(Path warehouseDir) {
    return super.sparkConf(warehouseDir)
        .set(
            "spark.sql.catalog.nessie.uri", String.format("http://127.0.0.1:%d/iceberg/", httpPort))
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.rest.RESTCatalog");
  }
}
