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

import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.util.SerializableMap;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.services.s3.S3Client;

@SuppressWarnings("unused") // loaded by Iceberg via java reflection
public class NessieS3ClientFactory implements S3FileIOAwsClientFactory {

  private Map<String, String> properties = Collections.emptyMap();
  private S3FileIOProperties fileIOProperties = new S3FileIOProperties();
  private HttpClientProperties httpClientProperties = new HttpClientProperties();
  private AwsClientProperties awsClientProperties = new AwsClientProperties();

  @Override
  public void initialize(Map<String, String> map) {
    properties = SerializableMap.copyOf(map);
    fileIOProperties = new S3FileIOProperties(map);
    httpClientProperties = new HttpClientProperties(map);
    awsClientProperties = new AwsClientProperties(map);
  }

  @Override
  public S3Client s3() {
    NessieS3Signer signer =
        fileIOProperties.isRemoteSigningEnabled() ? new NessieS3Signer(properties) : null;
    S3Client s3Client =
        S3Client.builder()
            .applyMutation(awsClientProperties::applyClientRegionConfiguration)
            .applyMutation(httpClientProperties::applyHttpClientConfigurations)
            .applyMutation(fileIOProperties::applyEndpointConfigurations)
            .applyMutation(fileIOProperties::applyServiceConfigurations)
            .applyMutation(
                b -> fileIOProperties.applyCredentialConfigurations(awsClientProperties, b))
            .applyMutation(
                builder -> {
                  if (fileIOProperties.isRemoteSigningEnabled()) {
                    builder.overrideConfiguration(
                        c -> c.putAdvancedOption(SdkAdvancedClientOption.SIGNER, signer));
                  }
                })
            .build();
    return new NessieS3Client(s3Client, signer);
  }
}
