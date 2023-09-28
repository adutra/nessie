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

import com.google.protobuf.gradle.ProtobufExtract

plugins {
  alias(libs.plugins.nessie.reflectionconfig)
  id("nessie-conventions-client")
  alias(libs.plugins.protobuf)
}

extra["maven.name"] = "Nessie - Catalog - gRPC (protobuf)"

description = "Nessie Catalog gRPC (protobuf) model."

dependencies {
  implementation(libs.protobuf.java)
  implementation(platform(libs.grpc.bom))
  implementation("io.grpc:grpc-protobuf")
  implementation("io.grpc:grpc-stub")

  // javax/jakarta
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.javax.annotation.api)
}

// *.proto files taken from https://github.com/googleapis/googleapis/ repo, available as a git
// submodule
protobuf {
  // Configure the protoc executable
  protoc {
    // Download from repositories
    artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}"
  }
  plugins {
    register("grpc").configure {
      artifact = "io.grpc:protoc-gen-grpc-java:${libs.grpc.bom.get().version}"
    }
  }
  generateProtoTasks { all().configureEach { plugins.register("grpc") } }
}

reflectionConfig {
  // Consider classes that extend one of these classes...
  classExtendsPatterns.set(
    listOf(
      "com.google.protobuf.GeneratedMessageV3",
      "com.google.protobuf.GeneratedMessageV3.Builder"
    )
  )
  // ... and classes the implement this interface.
  classImplementsPatterns.set(listOf("com.google.protobuf.ProtocolMessageEnum"))
  // Also include generated classes (e.g. google.protobuf.Empty) via the "runtimeClasspath",
  // which contains the the "com.google.protobuf:protobuf-java" dependency.
  includeConfigurations.set(listOf("runtimeClasspath"))
}

// The protobuf-plugin should ideally do this
tasks.named<Jar>("sourcesJar").configure {
  dependsOn(tasks.named("generateProto"), tasks.named("generateReflectionConfig"))
}

tasks.withType(ProtobufExtract::class).configureEach {
  when (name) {
    "extractIncludeTestProto" -> dependsOn(tasks.named("processJandexIndex"))
    "extractIncludeTestFixturesProto" -> dependsOn(tasks.named("processJandexIndex"))
    "extractIncludeIntTestProto" -> dependsOn(tasks.named("processJandexIndex"))
  }
}
