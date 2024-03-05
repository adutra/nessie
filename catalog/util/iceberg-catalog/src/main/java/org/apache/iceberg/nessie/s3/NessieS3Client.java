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
package org.apache.iceberg.nessie.s3;

import java.util.function.Function;
import javax.annotation.Nullable;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Request;

public class NessieS3Client extends DelegatingS3Client {

  public static final ExecutionAttribute<String> BUCKET_NAME_ATTRIBUTE =
      new ExecutionAttribute<>("Bucket");

  private final NessieS3Signer signer;

  public NessieS3Client(S3Client s3Client, @Nullable NessieS3Signer signer) {
    super(s3Client);
    this.signer = signer;
  }

  @Override
  protected <T extends S3Request, ReturnT> ReturnT invokeOperation(
      T request, Function<T, ReturnT> operation) {
    if (signer == null) {
      return super.invokeOperation(request, operation);
    }
    return super.invokeOperation(
        request
            .getValueForField("Bucket", String.class)
            .map(bucketName -> setBucketName(bucketName, request))
            .orElse(request),
        operation);
  }

  private <T extends S3Request> T setBucketName(String bucketName, T request) {
    @SuppressWarnings("unchecked")
    T overridden =
        (T)
            request.toBuilder()
                .overrideConfiguration(
                    request
                        .overrideConfiguration()
                        .map(AwsRequestOverrideConfiguration::toBuilder)
                        .orElseGet(AwsRequestOverrideConfiguration::builder)
                        .putExecutionAttribute(BUCKET_NAME_ATTRIBUTE, bucketName)
                        .build())
                .build();
    return overridden;
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      if (signer != null) {
        signer.close();
      }
    }
  }
}
