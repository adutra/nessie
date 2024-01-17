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
ICEBERG_VERSION="1.4.2"
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
  echo "  --old-catalog                   Use NessieCatalog instead of NessieCatalogIcebergCatalog. Default:false"
  echo "  --no-publish                    Do not publish jars to Maven local. Default: false"
  echo "  --no-clear-cache                Do not clear ivy cache. Default: false"
  echo "  --no-combined                   Do not use combined nessie core + catalog server. Default: false"
  echo "  --clear-warehouse               Clear warehouse directory. Default: false"
  echo "  --debug                         Enable debug mode"
  echo "  --verbose                       Enable verbose mode"
  echo "  --help                          Print this help"
  exit 0
fi

done

GRADLE_OPTS=("--quiet")

if [[ -z "$VERBOSE" ]]; then
  GRADLE_OPTS+=("--console=plain")
  REDIRECT="$PROJECT_DIR/build/spark-catalog-demo.log"
  NESSIE_CORE_PROMPT="[NESSIE CORE] "
  NESSIE_CATALOG_PROMPT="[NESSIE CTLG] "
  NESSIE_COMBINED_PROMPT="[NESSIE CMBD] "
else
  REDIRECT="/dev/stdout"
  NESSIE_CORE_PROMPT=$(printf "\033[1m\033[33m[NESSIE CORE] \033[0m")
  NESSIE_CATALOG_PROMPT=$(printf "\033[1m\033[32m[NESSIE CTLG] \033[0m")
  NESSIE_COMBINED_PROMPT=$(printf "\033[1m\033[32m[NESSIE CMBD] \033[0m")
fi

echo "Working directory  : $(pwd)"
echo "Warehouse location : $WAREHOUSE_LOCATION"
echo "Nessie logging to  : $REDIRECT"

if [[ -z "$NO_CLEAR_IVY_CACHE" ]]; then
  echo "Clearing Ivy cache..."
  rm -rf ~/.ivy2/cache/org.projectnessie
  rm -rf ~/.ivy2/cache/org.apache.iceberg
fi

if [[ -n "$CLEAR_WAREHOUSE" ]]; then
  echo "Clearing warehouse directory: $WAREHOUSE_LOCATION..."
  rm -rf "$WAREHOUSE_LOCATION"
fi

mkdir -p "$WAREHOUSE_LOCATION"

if [[ -z "$NO_PUBLISH_TO_MAVEN_LOCAL" ]]; then
  echo "Publishing to Maven local..."
  ./gradlew "${GRADLE_OPTS[@]}" publishToMavenLocal
fi

echo "Building nessie-core server..."
./gradlew "${GRADLE_OPTS[@]}" :nessie-quarkus:quarkusBuild


if [[ -z "$NO_COMBINED" ]]; then

  echo "Building combined nessie core + catalog server..."
  ./gradlew "${GRADLE_OPTS[@]}" :nessie-catalog-service-server-combined:quarkusBuild

else

  echo "Building nessie catalog server..."
  ./gradlew "${GRADLE_OPTS[@]}" :nessie-catalog-service-server:quarkusBuild

fi

if [[ -n "$DEBUG" ]]; then
  export QUARKUS_LOG_MIN_LEVEL="DEBUG"
  export QUARKUS_LOG_CONSOLE_LEVEL="DEBUG"
  export QUARKUS_LOG_CATEGORY__ORG_PROJECTNESSIE__LEVEL="DEBUG"
  export QUARKUS_LOG_CATEGORY__ORG_APACHE_ICEBERG__LEVEL="DEBUG"
  export QUARKUS_VERTX_MAX_EVENT_LOOP_EXECUTE_TIME="PT5M"
  DEBUG_NESSIE_CORE=(
    "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
  )
  DEBUG_NESSIE_CATALOG=(
    "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006"
    "-Dnessie.transport.read-timeout=300000"
  )
fi

if [[ -z "$NO_COMBINED" ]]; then

  echo "Starting combined nessie core + catalog server..."

  java "${DEBUG_NESSIE_CATALOG[@]}" \
    -Dquarkus.oidc.tenant-enabled=false -Dquarkus.otel.sdk.disabled=true \
    -jar catalog/service/server-combined/build/quarkus-app/quarkus-run.jar | \
    sed ''s/^/"$NESSIE_COMBINED_PROMPT"/'' \
    >> "$REDIRECT" 2>&1 &

  combined_pid=$!
  echo "Combined nessie core + catalog PID: $combined_pid"
  sleep 2
  echo "Waiting for combined nessie core + catalog server to start..."
  curl --silent --show-error --fail \
    --connect-timeout 5 --retry 5 --retry-connrefused --retry-delay 0 --retry-max-time 10 \
    http://localhost:19110/q/health/ready > /dev/null 2>&1

else

  echo "Starting nessie core server..."

  java "${DEBUG_NESSIE_CORE[@]}" \
    -Dquarkus.oidc.tenant-enabled=false -Dquarkus.otel.sdk.disabled=true \
    -jar servers/quarkus-server/build/quarkus-app/quarkus-run.jar | \
    sed  ''s/^/"$NESSIE_CORE_PROMPT"/'' \
    >> "$REDIRECT" 2>&1 &

  core_pid=$!
  echo "Nessie core PID: $core_pid"
  sleep 2
  echo "Waiting for nessie core to start..."
  curl --silent --show-error --fail \
    --connect-timeout 5 --retry 5 --retry-connrefused --retry-delay 0 --retry-max-time 10 \
    http://localhost:19120/q/health/ready > /dev/null 2>&1

  echo "Starting nessie catalog server..."

  java "${DEBUG_NESSIE_CATALOG[@]}" \
    -Dquarkus.oidc.tenant-enabled=false -Dquarkus.otel.sdk.disabled=true \
    -jar catalog/service/server/build/quarkus-app/quarkus-run.jar | \
    sed  ''s/^/"$NESSIE_CATALOG_PROMPT"/'' \
    >> "$REDIRECT" 2>&1 &

  catalog_pid=$!
  echo "Nessie catalog PID: $catalog_pid"
  sleep 2
  echo "Waiting for nessie catalog to start..."
  curl --silent --show-error --fail \
    --connect-timeout 5 --retry 5 --retry-connrefused --retry-delay 0 --retry-max-time 10 \
    http://localhost:19110/q/health/ready > /dev/null 2>&1

fi

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
