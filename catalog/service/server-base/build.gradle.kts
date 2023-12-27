/*
 * Copyright (C) 2022 Dremio
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

plugins {
  id("nessie-conventions-quarkus")
  id("nessie-jacoco")
}

extra["maven.name"] = "Nessie - Catalog - Server Base Artifact"

dependencies {
  implementation(project(":nessie-catalog-files-api"))
  implementation(project(":nessie-catalog-files-local"))
  implementation(project(":nessie-catalog-files-s3"))
  implementation(project(":nessie-catalog-schema-model"))
  implementation(project(":nessie-catalog-service-common"))
  implementation(project(":nessie-catalog-service-impl"))
  implementation(project(":nessie-catalog-service-rest"))
  implementation(project(":nessie-catalog-storage-backend"))
  implementation(project(":nessie-catalog-storage-persist"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-client"))
  implementation(project(":nessie-services-config"))

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-reactive-routes")
  implementation("io.quarkus:quarkus-resteasy-reactive")
  implementation("io.quarkus:quarkus-resteasy-reactive-jackson")
  implementation("io.quarkus:quarkus-hibernate-validator")
  implementation("io.smallrye:smallrye-open-api-jaxrs")
  implementation("io.quarkus:quarkus-smallrye-health")
  implementation("io.quarkus:quarkus-oidc")
  implementation("io.quarkus:quarkus-smallrye-openapi")
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-opentelemetry")
  implementation(libs.quarkus.logging.sentry)
  implementation("io.micrometer:micrometer-registry-prometheus")

  implementation(libs.guava)

  compileOnly(libs.microprofile.openapi)
}
