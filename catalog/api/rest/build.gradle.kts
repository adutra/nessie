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

import io.smallrye.openapi.api.OpenApiConfig.OperationIdStrategy
import io.smallrye.openapi.gradleplugin.SmallryeOpenApiTask

plugins {
  id("nessie-conventions-client")
  id("nessie-jacoco")
  alias(libs.plugins.annotations.stripper)
  alias(libs.plugins.smallrye.openapi)
}

extra["maven.name"] = "Nessie - Catalog - REST API"

description = "Nessie Catalog API implementation to a remote REST Nessie Catalog service."

dependencies {
  implementation(project(":nessie-catalog-schema-model"))
  implementation(project(":nessie-catalog-api-base"))
  implementation(project(":nessie-model"))

  implementation(libs.smallrye.config.core)

  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-guava")

  // javax/jakarta
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.javax.ws.rs)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.javax.enterprise.cdi.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
}

smallryeOpenApi {
  scanDependenciesDisable.set(true)
  infoTitle.set("Nessie Catalog")
  infoVersion.set(project.version.toString())
  infoDescription.set(
    "Transactional Catalog for Data Lakes\n" +
      "\n" +
      "* Git-inspired data version control\n" +
      "* Cross-table transactions and visibility\n" +
      "* Works with Apache Iceberg tables"
  )
  schemaFilename.set("META-INF/openapi/nessie-catalog")
  operationIdStrategy.set(OperationIdStrategy.METHOD)
  scanPackages.set(listOf("org.projectnessie.catalog.api.rest.spec"))
}

val openapiSource by
  configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
    description = "Source OpenAPI spec files, containing the examples"
  }

val generateOpenApiSpec = tasks.named<SmallryeOpenApiTask>("generateOpenApiSpec")

generateOpenApiSpec.configure {
  inputs.files("src/main").withPathSensitivity(PathSensitivity.RELATIVE)
}

artifacts { add(openapiSource.name, file("src/main/resources/META-INF")) }

annotationStripper {
  registerDefault().configure {
    annotationsToDrop("^jakarta[.].+".toRegex())
    unmodifiedClassesForJavaVersion.set(11)
  }
}
