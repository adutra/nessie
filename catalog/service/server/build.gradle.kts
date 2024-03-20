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
  alias(libs.plugins.nessie.run)
}

extra["maven.name"] = "Nessie - Catalog - Server with Nessie Core"

description = "Nessie Catalog Server with Nessie Core (Quarkus)"

val quarkusRunner by
  configurations.creating {
    description = "Used to reference the generated runner-jar (either fast-jar or uber-jar)"
  }

val openapiSource by
  configurations.creating { description = "Used to reference OpenAPI spec files" }

val versionIceberg = libs.versions.iceberg.get()

dependencies {
  // Nessie Core
  implementation(project(":nessie-client"))
  implementation(project(":nessie-combined-cs"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-quarkus-auth"))
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-rest-common"))
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-tasks-api"))
  implementation(project(":nessie-tasks-service-async"))
  implementation(project(":nessie-tasks-service-impl"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-catalog-files-api"))
  implementation(project(":nessie-catalog-files-impl"))
  implementation(project(":nessie-catalog-schema-model"))
  implementation(project(":nessie-catalog-service-common"))
  implementation(project(":nessie-catalog-service-impl"))
  implementation(project(":nessie-catalog-service-rest"))

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-reactive-routes")
  implementation("io.quarkus:quarkus-resteasy-reactive")
  implementation("io.quarkus:quarkus-resteasy-reactive-jackson")
  implementation("io.quarkus:quarkus-smallrye-context-propagation")
  implementation("io.quarkus:quarkus-hibernate-validator")
  implementation("io.smallrye:smallrye-open-api-jaxrs")
  implementation("io.quarkus:quarkus-smallrye-health")
  implementation("io.quarkus:quarkus-smallrye-openapi")
  implementation("io.quarkus:quarkus-security")
  implementation("io.quarkus:quarkus-oidc")
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-opentelemetry")
  implementation(libs.quarkus.logging.sentry)
  implementation("io.micrometer:micrometer-registry-prometheus")

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:s3")

  implementation(libs.guava)

  openapiSource(project(":nessie-catalog-api-rest", "openapiSource"))

  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(enforcedPlatform(libs.quarkus.bom))
  testFixturesApi("io.quarkus:quarkus-junit5")
  testFixturesApi("io.quarkus:quarkus-jacoco")

  testFixturesApi(project(":nessie-catalog-service-server-tests"))

  testFixturesCompileOnly(libs.microprofile.openapi)

  testFixturesImplementation(platform(libs.junit.bom))
  testFixturesImplementation(libs.bundles.junit.testing)

  intTestImplementation(project(":nessie-s3minio"))
  intTestImplementation(platform(libs.awssdk.bom))
  intTestImplementation("software.amazon.awssdk:s3")
  intTestImplementation("software.amazon.awssdk:sts")
  intTestImplementation(platform(libs.testcontainers.bom))
  intTestImplementation("org.testcontainers:testcontainers")

  intTestImplementation("org.apache.iceberg:iceberg-core:$versionIceberg")
  intTestImplementation("org.apache.iceberg:iceberg-bundled-guava:$versionIceberg")
  intTestImplementation("org.apache.iceberg:iceberg-aws:$versionIceberg")
  intTestImplementation("org.apache.iceberg:iceberg-nessie:$versionIceberg")
  intTestImplementation("org.apache.iceberg:iceberg-api:$versionIceberg:tests")
  intTestImplementation("org.apache.iceberg:iceberg-core:$versionIceberg:tests")
  intTestImplementation(libs.hadoop.common) { hadoopExcludes() }
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

val sparkScala = useSparkScalaVersionsForProject("3.5", "2.13")

testing {
  suites {
    register<JvmTestSuite>("sparkIntTest") {
      useJUnitJupiter(libsRequiredVersion("junit"))

      testType.set("spark-test")

      dependencies {
        implementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
          forSpark(sparkScala.sparkVersion)
        }
        implementation("org.apache.spark:spark-core_${sparkScala.scalaMajorVersion}") {
          forSpark(sparkScala.sparkVersion)
        }
        implementation("org.apache.spark:spark-hive_${sparkScala.scalaMajorVersion}") {
          forSpark(sparkScala.sparkVersion)
        }

        implementation(libs.assertj.core)
        compileOnly(libs.errorprone.annotations)

        implementation(
          "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:${libs.versions.iceberg.get()}"
        )
        implementation("org.apache.iceberg:iceberg-nessie:${libs.versions.iceberg.get()}")
      }

      targets.all {
        testTask.configure {

          // Remove quarkus-specific log manager overrides
          systemProperties.remove("java.util.logging.manager")

          usesService(
            gradle.sharedServices.registrations.named("intTestParallelismConstraint").get().service
          )

          dependsOn("quarkusBuild")

          shouldRunAfter("test")

          forkEvery = 1

          forceJavaVersion(sparkScala.runtimeJavaVersion)
        }

        tasks.named("check").configure { dependsOn(testTask) }
      }
    }
  }
}

nessieQuarkusApp {
  includeTask(tasks.named<Test>("sparkIntTest"))
  executableJar.convention {
    project.layout.buildDirectory.file("quarkus-app/quarkus-run.jar").get().asFile
  }
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
  jvmArgumentsNonInput.add("-XX:SelfDestructTimer=30")
  systemProperties.put("nessie.server.send-stacktrace-to-client", "true")
}

fun ModuleDependency.hadoopExcludes() {
  exclude("ch.qos.reload4j", "reload4j")
  exclude("com.sun.jersey")
  exclude("commons-cli", "commons-cli")
  exclude("jakarta.activation", "jakarta.activation-api")
  exclude("javax.servlet", "javax.servlet-api")
  exclude("javax.servlet.jsp", "jsp-api")
  exclude("javax.ws.rs", "javax.ws.rs-api")
  exclude("log4j", "log4j")
  exclude("org.slf4j", "slf4j-log4j12")
  exclude("org.slf4j", "slf4j-reload4j")
  exclude("org.eclipse.jetty")
  exclude("org.apache.zookeeper")
}
