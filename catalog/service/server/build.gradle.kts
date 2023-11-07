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

import io.quarkus.gradle.tasks.QuarkusBuild
import org.apache.tools.ant.taskdefs.condition.Os

plugins {
  alias(libs.plugins.quarkus)
  id("nessie-conventions-quarkus")
  id("nessie-jacoco")
}

extra["maven.name"] = "Nessie - Catalog - Server"

description = "Nessie Catalog Server (Quarkus)"

val quarkusRunner by
  configurations.creating {
    description = "Used to reference the generated runner-jar (either fast-jar or uber-jar)"
  }

val openapiSource by
  configurations.creating { description = "Used to reference OpenAPI spec files" }

dependencies {
  implementation(project(":nessie-catalog-files-api"))
  implementation(project(":nessie-catalog-files-local"))
  implementation(project(":nessie-catalog-schema-model"))
  implementation(project(":nessie-catalog-service-impl"))
  implementation(project(":nessie-catalog-service-rest"))
  implementation(project(":nessie-catalog-storage-backend"))
  implementation(project(":nessie-catalog-storage-persist"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-inmemory"))
  implementation(project(":nessie-client"))

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

  openapiSource(project(":nessie-catalog-api-rest", "openapiSource"))

  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(enforcedPlatform(libs.quarkus.bom))
  testFixturesApi("io.quarkus:quarkus-junit5")
  testFixturesApi("io.quarkus:quarkus-jacoco")

  testFixturesCompileOnly(libs.microprofile.openapi)

  testFixturesImplementation(platform(libs.junit.bom))
  testFixturesImplementation(libs.bundles.junit.testing)
}

val pullOpenApiSpec by tasks.registering(Sync::class)

pullOpenApiSpec.configure {
  destinationDir = openApiSpecDir
  from(openapiSource) { include("openapi.yaml") }
}

val openApiSpecDir = layout.buildDirectory.asFile.map { it.resolve("openapi-extra") }.get()

quarkus {
  quarkusBuildProperties.put("quarkus.package.type", quarkusPackageType())
  quarkusBuildProperties.put(
    "quarkus.smallrye-openapi.store-schema-directory",
    layout.buildDirectory.asFile.map { it.resolve("openapi") }.get().toString()
  )
  quarkusBuildProperties.put(
    "quarkus.smallrye-openapi.additional-docs-directory",
    openApiSpecDir.toString()
  )
  quarkusBuildProperties.put("quarkus.smallrye-openapi.info-version", project.version.toString())
  quarkusBuildProperties.put("quarkus.smallrye-openapi.auto-add-security", "false")
}

val quarkusAppPartsBuild = tasks.named("quarkusAppPartsBuild")

quarkusAppPartsBuild.configure {
  dependsOn(pullOpenApiSpec)
  inputs.files(openapiSource)
}

val quarkusBuild = tasks.named<QuarkusBuild>("quarkusBuild")

// Expose runnable jar via quarkusRunner configuration for integration-tests that require the
// server.
artifacts {
  add(
    quarkusRunner.name,
    provider {
      if (quarkusFatJar()) quarkusBuild.get().runnerJar
      else quarkusBuild.get().fastJar.resolve("quarkus-run.jar")
    }
  ) {
    builtBy(quarkusBuild)
  }
}

// Add the uber-jar, if built, to the Maven publication
if (quarkusFatJar()) {
  afterEvaluate {
    publishing {
      publications {
        named<MavenPublication>("maven") {
          artifact(quarkusBuild.get().runnerJar) {
            classifier = "runner"
            builtBy(quarkusBuild)
          }
        }
      }
    }
  }
}

listOf("javadoc", "sourcesJar").forEach { name ->
  tasks.named(name) { dependsOn(tasks.named("compileQuarkusGeneratedSourcesJava")) }
}

listOf("checkstyleTest", "compileTestJava").forEach { name ->
  tasks.named(name) { dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava")) }
}

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.withType<Test>().configureEach { this.enabled = false }
}
