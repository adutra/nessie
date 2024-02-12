#!/usr/bin/env bash
#
# Copyright (C) 2024 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
trap 'kill $(jobs -p)' EXIT
set -e

PROJECT_DIR=$(dirname "$0")/../..
cd "$PROJECT_DIR"
PROJECT_DIR=$(pwd)

# Set the default values
NESSIE_VERSION=$(./gradlew properties -q | awk '/^version:/ {print $2}')
ICEBERG_VERSION="1.4.3"
SPARK_VERSION="3.5"
SCALA_VERSION="2.12"
AWS_SDK_VERSION="2.20.131"
WAREHOUSE_LOCATION="$PROJECT_DIR/build/spark-warehouse"

# Parse the command line arguments
while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in
    --nessie-version)
      NESSIE_VERSION="$2"
      shift
      shift
      ;;
    --iceberg-version)
      ICEBERG_VERSION="$2"
      shift
      shift
      ;;
    --spark-version)
      SPARK_VERSION="$2"
      shift
      shift
      ;;
    --scala-version)
      SCALA_VERSION="$2"
      shift
      shift
      ;;
    --aws-sdk-version)
      AWS_SDK_VERSION="$2"
      shift
      shift
      ;;
    --warehouse)
      WAREHOUSE_LOCATION="$2"
      shift
      shift
      ;;
    --old-catalog)
      USE_OLD_CATALOG="true"
      shift
      ;;
    --no-publish)
      NO_PUBLISH_TO_MAVEN_LOCAL="true"
      shift
      ;;
    --no-clear-cache)
      NO_CLEAR_IVY_CACHE="true"
      shift
      ;;
    --no-nessie-start)
      NO_NESSIE_START="true"
      shift
      ;;
    --no-combined)
      NO_COMBINED="true"
      shift
      ;;
    --clear-warehouse)
      CLEAR_WAREHOUSE="true"
      shift
      ;;
    --aws)
      AWS="true"
      shift
      ;;
    --debug)
      DEBUG="true"
      shift
      ;;
    --verbose)
      VERBOSE="true"
      shift
      ;;
    --help)
      HELP="true"
      shift
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac

if [[ -n "$HELP" ]]; then
  echo "Usage: spark-sql.sh [options]"
  echo "Options:"
  echo "  --nessie-version <version>      Nessie version to use. Default: $NESSIE_VERSION"
  echo "  --iceberg-version <version>     Iceberg version to use. Default: $ICEBERG_VERSION"
  echo "  --spark-version <version>       Spark version to use. Default: $SPARK_VERSION"
  echo "  --scala-version <version>       Scala version to use. Default: $SCALA_VERSION"
  echo "  --aws-sdk-version <version>     AWS SDK version to use. Default: $AWS_SDK_VERSION"
  echo "  --warehouse <location>          Warehouse location. Default: $WAREHOUSE_LOCATION"
  echo "  --old-catalog                   Use NessieCatalog instead of NessieCatalogIcebergCatalog. Default: use Nessie Catalog"
  echo "  --no-publish                    Do not publish jars to Maven local. Default: false"
  echo "  --no-clear-cache                Do not clear ivy cache. Default: false"
  echo "  --no-combined                   Do not use combined Nessie Core + Catalog server. Default: combined"
  echo "  --no-start                      Do not start Nessie Core/Catalog, use externally provided instance(s). Default: start"
  echo "  --clear-warehouse               Clear warehouse directory. Default: false"
  echo "  --debug                         Enable debug mode"
  echo "  --verbose                       Enable verbose mode"
  echo "  --help                          Print this help"
  exit 0
fi

done

source "$PROJECT_DIR/catalog/bin/common.sh"

PACKAGES=(
  "org.apache.iceberg:iceberg-spark-${SPARK_VERSION}_${SCALA_VERSION}:${ICEBERG_VERSION}"
  "org.apache.iceberg:iceberg-nessie:${ICEBERG_VERSION}"
  "org.projectnessie.nessie:nessie-model:${NESSIE_VERSION}"
  "org.projectnessie.nessie:nessie-client:${NESSIE_VERSION}"
)

if [[ -z "$USE_OLD_CATALOG" ]]; then
  CATALOG_IMPL="org.apache.iceberg.nessie.NessieCatalogIcebergCatalog"
  PACKAGES+=(
    "org.projectnessie.nessie:nessie-catalog-iceberg-catalog:${NESSIE_VERSION}"
  )
else
  CATALOG_IMPL="org.apache.iceberg.nessie.NessieCatalog"
fi

if [[ -n "$AWS" ]]; then
  PACKAGES+=(
    "software.amazon.awssdk:bundle:${AWS_SDK_VERSION}"
    "software.amazon.awssdk:url-connection-client:${AWS_SDK_VERSION}"
  )
fi

if [[ -n "$DEBUG" ]]; then
  DEBUG_SPARK_SHELL=(
    "--conf" "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007"
    "--conf" "spark.sql.catalog.nessie.transport.read-timeout=300000"
  )
fi

echo
echo "Starting spark-sql $SPARK_VERSION (catalog impl: $CATALOG_IMPL)..."

echo "Packages:"
for package in "${PACKAGES[@]}"; do
  echo "- $package"
done

packages_csv=$(printf ",%s" "${PACKAGES[@]}")
packages_csv=${packages_csv:1}

spark-sql "${DEBUG_SPARK_SHELL[@]}" \
  --packages "${packages_csv}" \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf spark.sql.catalog.nessie.uri=http://127.0.0.1:19110/api/v2 \
  --conf spark.sql.catalog.nessie.client-api-version=2 \
  --conf spark.sql.catalog.nessie.ref=main \
  --conf spark.sql.catalog.nessie.catalog-impl="$CATALOG_IMPL" \
  --conf spark.sql.catalog.nessie.warehouse="$WAREHOUSE_LOCATION" \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog
