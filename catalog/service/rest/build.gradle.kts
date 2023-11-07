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
  id("nessie-conventions-server")
  id("nessie-jacoco")
  alias(libs.plugins.annotations.stripper)
}

extra["maven.name"] = "Nessie - Catalog - REST Service"

description = "Nessie Catalog service implementation providing REST endpoints."

dependencies {
  implementation(project(":nessie-catalog-api-base"))
  implementation(project(":nessie-catalog-api-rest"))
  implementation(project(":nessie-catalog-schema-model"))
  implementation(project(":nessie-catalog-service-api"))
  implementation(project(":nessie-catalog-service-common"))
  implementation(project(":nessie-model"))
  implementation(project(":nessie-client"))

  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  implementation(libs.slf4j.api)
  implementation(libs.guava)

  // javax/jakarta
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(project(":nessie-combined-cs"))
  testFixturesApi(project(":nessie-catalog-storage-inmemory"))
}

annotationStripper {
  registerDefault().configure {
    annotationsToDrop("^jakarta[.].+".toRegex())
    unmodifiedClassesForJavaVersion.set(11)
  }
}
