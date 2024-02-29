/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.files.s3;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.s3mock.IcebergS3Mock;
import org.projectnessie.s3mock.MockObject;
import org.projectnessie.s3mock.S3Bucket;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@ExtendWith(SoftAssertionsExtension.class)
public class TestS3Clients {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void baseClient() throws Exception {
    String answer1 = "hello world ";
    String answer2 = "hello other ";
    String bucket1 = "mybucket";
    String bucket2 = "otherbucket";
    try (IcebergS3Mock.S3MockServer server1 =
            IcebergS3Mock.builder()
                .putBuckets(
                    bucket1,
                    S3Bucket.builder()
                        .object(
                            key ->
                                MockObject.builder()
                                    .contentLength(answer1.length() + key.length())
                                    .writer(
                                        (range, output) ->
                                            output.write((answer1 + key).getBytes(UTF_8)))
                                    .contentType("text/plain")
                                    .build())
                        .build())
                .build()
                .start();
        IcebergS3Mock.S3MockServer server2 =
            IcebergS3Mock.builder()
                .putBuckets(
                    bucket2,
                    S3Bucket.builder()
                        .object(
                            key ->
                                MockObject.builder()
                                    .contentLength(answer2.length() + key.length())
                                    .writer(
                                        (range, output) ->
                                            output.write((answer2 + key).getBytes(UTF_8)))
                                    .contentType("text/plain")
                                    .build())
                        .build())
                .build()
                .start()) {

      S3Client baseClient = S3Clients.createS3BaseClient(S3Config.builder().build());

      S3ProgrammaticOptions s3options =
          S3ProgrammaticOptions.builder()
              .cloud(Cloud.PRIVATE)
              .putBuckets(
                  bucket1,
                  S3ProgrammaticOptions.S3PerBucketOptions.builder()
                      .endpoint(server1.getBaseUri())
                      .region("us-west-1")
                      .accessKeyIdRef("ak1")
                      .secretAccessKeyRef("sak1")
                      .build())
              .putBuckets(
                  bucket2,
                  S3ProgrammaticOptions.S3PerBucketOptions.builder()
                      .endpoint(server2.getBaseUri())
                      .region("eu-central-2")
                      .accessKeyIdRef("ak2")
                      .secretAccessKeyRef("sak2")
                      .build())
              .build();

      S3Client configuredClient =
          S3Clients.configuredClient(baseClient, s3options, secret -> "secret");

      String key1 = "meep";
      String key2 = "blah";
      String response1;
      String response2;
      try (ResponseInputStream<GetObjectResponse> input =
          configuredClient.getObject(
              GetObjectRequest.builder().bucket(bucket1).key(key1).build())) {
        response1 = new String(input.readAllBytes());
      }
      try (ResponseInputStream<GetObjectResponse> input =
          configuredClient.getObject(
              GetObjectRequest.builder().bucket(bucket2).key(key2).build())) {
        response2 = new String(input.readAllBytes());
      }
      soft.assertThat(response1).isEqualTo(answer1 + key1);
      soft.assertThat(response2).isEqualTo(answer2 + key2);
    }
  }
}
