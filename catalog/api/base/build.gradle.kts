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
  id("nessie-conventions-client")
  id("nessie-jacoco")
  alias(libs.plugins.annotations.stripper)
}

extra["maven.name"] = "Nessie - Catalog - API Base"

description = "Nessie - Catalog - API Base, bridge between transport protocols and catalog models"

dependencies {
  implementation(project(":nessie-catalog-schema-model"))
  implementation(project(":nessie-client"))

  implementation(libs.smallrye.config.core)
  // implementation(libs.smallrye.config.crypto) // only since Smallrye config >= 3.2.0
  implementation(libs.smallrye.config.validator)
  implementation(libs.smallrye.config.source.yaml)

  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")

  // javax/jakarta
  implementation(libs.javax.annotation.api) // 'implementation' for smallrye-config
  implementation(libs.jakarta.annotation.api) // 'implementation' for smallrye-config
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
}

annotationStripper {
  registerDefault().configure {
    annotationsToDrop("^jakarta[.].+".toRegex())
    unmodifiedClassesForJavaVersion.set(11)
  }
}
