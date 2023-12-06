# TODOs

## General

* Do not use `java.nio.ByteBuffer` in (value) objects

## Data files/manifests/etc

* Can we maintain references to all data files in the catalog?
    * How much data would it consume?
    * What is a good serialization scheme for the catalog data?
        * Should different serialization methods for data files/manifests and "other" catalog data be used?
* Identify how "predicate push-down" could work
    * Consider partition/bucket functions (Iceberg, Delta?)
    * Consider varying partition specs
    * Consider min/max values (per-data file statistics)
    * Can we use CEL to push-down predicates? (Think: possible data types)
* Bucketing/partitioning - can we define a superset of bucketing functionality? Is it worth the effort?
    * Additional bucketing functions could be:
        * Extract timestamp from Time-UUIDs
        * Substring ("starts with")
    * Would need support in the software performing the DML operations.
    * Can we define a common subset of Iceberg and Delta?

## Transparently migrate to Nessie Catalog

* Accessing tables that exist only in S3
* Let the Nessie catalog
* Define the "source of truth" during the migration to Nessie catalog
    * Is it possible to work against the "plain data lake" _and_ the Nessie catalog concurrently (during the migration)?
    * Is it worth to be able to (permanently) mark a table (or view) as "pure data lake only"
* Have the Nessie catalog S3 endpoint work as "pass through only"
* Fully migrating a table including **all versions** (Nessie commits) of a table to the Nessie Catalog will take a
  while. Nessie Catalog must be able to cope with this to provide a seamless migration phase & experience.
* Duplicate migration jobs for the same table should be prevented, but might occur. The (migration) system must be able
  to deal with such a situation.

### Ideas

* Have a lookup functionality from an Iceberg table-metadata pointer to the Nessie Catalog snapshot information to
  transparently migrate to the Nessie Catalog.

### Migration steps

Any table (or view) can be accessed through Nessie Catalog's S3 endpoint. Tables that (only) exist in the data lake
would automatically be migrated to the Nessie Catalog.

1. Configure the Nessie Catalog's S3 endpoint to work in "pass though only" mode. This means, that the Nessie Catalog
   will be updated, but all metadata files will also be written to the data lake.
2. Update all applications to use the Nessie Catalog. Either using a Nessie Catalog implementation for example for
   Spark/Iceberg or point the applications to the Nessie Catalog S3 endpoint.
3. Update the Nessie Catalog's S3 endpoint to switch to "normal operations" (i.e. to "only" "convert" updates to tables
   to updates to the catalog). 

## PoC

1. In a terminal
   ```bash
   ./gradlew :nessie-quarkus:quarkusBuild && java -jar servers/quarkus-server/build/quarkus-app/quarkus-run.jar
   ```
1. In a second terminal
   ```bash
   ./gradlew :nessie-catalog-service-server:quarkusBuild && java -jar catalog/service/server/build/quarkus-app/quarkus-run.jar
   ```
1. **ALTERNATIVE W/ NESSIE CATALOG INTEGRATED**
   1. Without SQL extensions - but **with Nessie Catalog aware Iceberg**.
      This approach "integrates better" with Nessie Catalog. It redirects table metadata and manifest lists to the Nessie Catalog.
      In other words it allows direct use of `INSERT`/`UPDATE`/`DELETE`/`SELECT`/etc working with Nessie Catalog.
      Since all SQL extensions use _relocated_ classes and the Iceberg catalog implementation for Nessie Catalog relies on non-relocated classes, the SQL extensions cannot be used in this early stage.
      ```bash
      rm -rf /tmp/nessie-catalog-demo
      mkdir -p /tmp/nessie-catalog-demo
      ./gradlew publishToMavenLocal

      nessieVersion=$(./gradlew properties -q | awk '/^version:/ {print $2}')
      icebergVersion=1.4.2
      sparkVersion=3.5
      scalaVersion=2.12

      packages=$(echo \
        org.apache.iceberg:iceberg-spark-${sparkVersion}_${scalaVersion}:${icebergVersion} \
        org.projectnessie.nessie:nessie-catalog-iceberg-catalog:$nessieVersion \
        | sed "s/ /,/g")

      spark-sql \
        --packages "${packages}" \
        --conf spark.sql.catalog.nessie.uri=http://127.0.0.1:19110/api/v2 \
        --conf spark.sql.catalog.nessie.ref=main \
        --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalogIcebergCatalog \
        --conf spark.sql.catalog.nessie.warehouse=/tmp/nessie-catalog-demo \
        --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog
      ```
      Add the following lines to `$SPARK_HOME/conf/log4j2.properties` to get some more information about what the adopted catalog implementation does:
      ```bash
      cp $SPARK_HOME/conf/log4j2.properties.template $SPARK_HOME/conf/log4j2.properties
      cat <<! >>$SPARK_HOME/conf/log4j2.properties

      logger.iceberg.name = org.apache.iceberg
      logger.iceberg.level = info

      logger.nessie.name = org.projectnessie
      logger.nessie.level = info
      !
      ```
      Then, in Spark SQL:
      ```sql
      CREATE NAMESPACE nessie.testing;

      CREATE TABLE nessie.testing.city (
        C_CITYKEY BIGINT, C_NAME STRING, N_NATIONKEY BIGINT, C_COMMENT STRING
      ) USING iceberg PARTITIONED BY (bucket(16, N_NATIONKEY));

      INSERT INTO nessie.testing.city VALUES (1, 'a', 1, 'comment');
     
      -- and so on...
      ```
   1. The functionality to inspect responses discussed below (curl + wget examples), work as well.
1. **ALTERNATIVE W/O NESSIE CATALOG INTEGRATED**
   1. In a third terminal
      1. With SQL extensions - but **without Nessie Catalog aware Iceberg**. 
         ```bash
         rm -rf /tmp/nessie-catalog-demo
         mkdir -p /tmp/nessie-catalog-demo
         ./gradlew publishToMavenLocal

         nessieVersion=$(./gradlew properties -q | awk '/^version:/ {print $2}')
         icebergVersion=1.4.2
         sparkVersion=3.5
         scalaVersion=2.12

         packages=$(echo \
         org.apache.iceberg:iceberg-spark-runtime-${sparkVersion}_${scalaVersion}:${icebergVersion} \
         org.projectnessie.nessie-integrations:nessie-spark-extensions-${sparkVersion}_${scalaVersion}:$nessieVersion \
         org.projectnessie.nessie:nessie-catalog-iceberg-httpfileio:$nessieVersion \
         | sed "s/ /,/g")

         spark-sql \
         --packages "${packages}" \
         --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
         --conf spark.sql.catalog.nessie.uri=http://127.0.0.1:19120/api/v1 \
         --conf spark.sql.catalog.nessie.ref=main \
         --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
         --conf spark.sql.catalog.nessie.warehouse=/tmp/nessie-catalog-demo \
         --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
         --conf spark.sql.catalog.nessie.io-impl=org.projectnessie.catalog.iceberg.httpfileio.HttpFileIO
         ```
   1. In Spark-SQL:
      ```sql
      CREATE NAMESPACE nessie.testing;

      CREATE TABLE nessie.testing.city (
       C_CITYKEY BIGINT, C_NAME STRING, N_NATIONKEY BIGINT, C_COMMENT STRING
      ) USING iceberg PARTITIONED BY (bucket(16, N_NATIONKEY));
      ```
   1. In a terminal:
      ```bash
      curl 'http://127.0.0.1:19110/catalog/v1/trees/main/snapshot/testing.city?format=iceberg' | jq
      curl 'http://127.0.0.1:19110/catalog/v1/trees/main/snapshot/testing.city' | jq
      ```
   1. In Spark-SQL:
      ```sql
      INSERT INTO nessie.testing.city VALUES (1, 'a', 1, 'comment');
      ```
   1. In a terminal:
      ```bash
      curl --compressed 'http://127.0.0.1:19110/catalog/v1/trees/main/snapshot/testing.city?format=iceberg' | jq
      curl --compressed 'http://127.0.0.1:19110/catalog/v1/trees/main/snapshot/testing.city' | jq
      ```
   1. In Spark-SQL:
      ```sql
      INSERT INTO nessie.testing.city VALUES
        (2, 'b', 2, 'commentb'),
        (3, 'c', 3, 'comment c'),
        (4, 'd', 4, 'comment d'),
        (5, 'e', 5, 'comment e');
      ```
   1. In a terminal:
      ```bash
      curl --compressed 'http://127.0.0.1:19110/catalog/v1/trees/main/snapshot/testing.city?format=iceberg' | jq
      curl --compressed 'http://127.0.0.1:19110/catalog/v1/trees/main/snapshot/testing.city' | jq
      ```
1. Inspect an Iceberg manifest list:
   ```
   wget --content-disposition 'http://127.0.0.1:19110/catalog/v1/trees/main/manifest-list/testing.city'
   java -jar avro-tools-1.11.3.jar getschema #USE_DOWNLOADED_FILE_NAME
   java -jar avro-tools-1.11.3.jar getmeta #USE_DOWNLOADED_FILE_NAME
   java -jar avro-tools-1.11.3.jar tojson #USE_DOWNLOADED_FILE_NAME
   ```
1. Inspect an Iceberg manifest file:
   ```
   wget --content-disposition 'http://127.0.0.1:19110/catalog/v1/trees/main/manifest-file/testing.city?manifest-file=<BASE_64_NESSIE_ID_OF_THE_MANIFEST_FILE>'
   java -jar avro-tools-1.11.3.jar getschema #USE_DOWNLOADED_FILE_NAME
   java -jar avro-tools-1.11.3.jar getmeta #USE_DOWNLOADED_FILE_NAME
   java -jar avro-tools-1.11.3.jar tojson #USE_DOWNLOADED_FILE_NAME
   ```
1. Perform a `SELECT` using table-metadata, manifest-list and manifest-files from the Nessie Data Catalog
   1. Replace the table-metadata JSON file with the table-metadata from NDC:
      ```bash
      cd /tmp/nessie-catalog-demo/testing/city_*/metadata
      # Some safety net
      tar cf backup.tar .
      # Replace the table-metadata JSON
      curl 'http://127.0.0.1:19110/catalog/v1/trees/main/snapshot/testing.city?format=iceberg' > 00002-*.json
      # Delete the .crc file
      rm .00002-*
      ```
   2. Re-start the Spark SQL REPL (see above), but keep the temp directory
   3. In Spark SQL run a `SELECT` - it will use the table-metadata as generated by NDC and pull the manifest-list
      and manifest-files from NDC via HTTP.
      ```sql
      SELECT * FROM nessie.testing.city;
      ```

### Avro-Tools

The `avro-tools` jar can be downloaded from [this location](https://dlcdn.apache.org/avro/avro-1.11.3/java/avro-tools-1.11.3.jar).
