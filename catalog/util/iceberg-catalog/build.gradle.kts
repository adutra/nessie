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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("nessie-conventions-client")
  id("nessie-jacoco")
  // FIXME see https://github.com/johnrengelman/shadow/pull/876
  // id("com.github.johnrengelman.shadow")
  id("io.github.goooler.shadow")
}

description = "Nessie - Catalog - Iceberg Client"

val versionIceberg = libs.versions.iceberg.get()

dependencies {
  implementation(libs.slf4j.api)
  compileOnly(libs.hadoop.common)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  implementation(nessieProject("nessie-client"))
  implementation(nessieProject("nessie-catalog-iceberg-httpfileio"))

  // Iceberg jars are deployed independently, e.g. in the fork of Iceberg Spark extensions.
  compileOnly("org.apache.iceberg:iceberg-core:$versionIceberg")
  compileOnly("org.apache.iceberg:iceberg-bundled-guava:$versionIceberg")
  compileOnly("org.apache.iceberg:iceberg-nessie:$versionIceberg")
}

tasks.named<ShadowJar>("shadowJar").configure {
  // Relocate fasterxml to be compatible with Iceberg Spark artefacts.
  // Note: this duplicates the relocated fasterxml classes on top of Iceberg Spark jars,
  // but the end result appears to be runtime compatible.
  relocate("com.fasterxml", "org.apache.iceberg.shaded.com.fasterxml")
}
