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

import static org.apache.iceberg.nessie.s3.NessieS3Client.BUCKET_NAME_ATTRIBUTE;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.nessie.NessieCatalogTableOperations;
import org.apache.iceberg.nessie.NessieUtil;
import org.projectnessie.catalog.api.sign.ImmutableSigningRequest;
import org.projectnessie.catalog.api.sign.SigningRequest;
import org.projectnessie.catalog.api.sign.SigningResponse;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.config.NessieClientConfigSource;
import org.projectnessie.client.config.NessieClientConfigSources;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.impl.UriBuilder;
import software.amazon.awssdk.auth.signer.internal.AbstractAws4Signer;
import software.amazon.awssdk.auth.signer.internal.Aws4SignerRequestParams;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.core.checksums.SdkChecksum;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.utils.IoUtils;

/**
 * An implementation of {@link AbstractAws4Signer} that does not have local access to S3
 * credentials, but delegates request signing to the Nessie Catalog server.
 */
public class NessieS3Signer extends AbstractAws4Signer<AwsS3V4SignerParams, Aws4PresignerParams>
    implements AutoCloseable {

  private final Map<String, String> options;
  private final String contentKey;
  private final String ref;
  private HttpClient httpClient;
  private URI baseUri;

  public NessieS3Signer(Map<String, String> options) {
    this.options = options;
    contentKey = options.get(NessieCatalogTableOperations.NESSIE_CONTENT_KEY_PROPERTY);
    if (contentKey == null) {
      throw new IllegalArgumentException(
          "Nessie content key must be set in the options: "
              + NessieCatalogTableOperations.NESSIE_CONTENT_KEY_PROPERTY);
    }
    ref = options.get(NessieCatalogTableOperations.NESSIE_CURRENT_REF_PROPERTY);
    if (ref == null) {
      throw new IllegalArgumentException(
          "Nessie current hash must be set in the options: "
              + NessieCatalogTableOperations.NESSIE_CURRENT_REF_PROPERTY);
    }
  }

  private HttpRequest newRequest() {
    if (httpClient == null) {
      NessieClientConfigSource configSource =
          NessieClientConfigSources.mapConfigSource(options)
              .fallbackTo(x -> options.get(x.replace(NessieUtil.NESSIE_CONFIG_PREFIX, "")));

      httpClient =
          NessieClientBuilder.createClientBuilderFromSystemSettings(configSource)
              .withApiCompatibilityCheck(false)
              .build(NessieApiV2.class)
              .unwrapClient(HttpClient.class)
              .orElseThrow(() -> new IllegalStateException("Nessie client must be HTTP"));

      baseUri =
          new UriBuilder(httpClient.getBaseUri().resolve("/"))
              .path("/catalog/v1/trees/{ref}/sign/{key}")
              .resolveTemplate("ref", ref)
              .resolveTemplate("key", contentKey)
              .build();
    }

    return httpClient.newRequest(baseUri);
  }

  @Override
  public SdkHttpFullRequest sign(SdkHttpFullRequest sdkRequest, ExecutionAttributes attributes) {

    AwsS3V4SignerParams params =
        extractSignerParams(AwsS3V4SignerParams.builder(), attributes).build();
    SigningRequest request =
        ImmutableSigningRequest.builder()
            .uri(sdkRequest.getUri())
            .method(sdkRequest.method().name())
            .region(params.signingRegion().id())
            .bucket(attributes.getAttribute(BUCKET_NAME_ATTRIBUTE))
            .headers(sdkRequest.headers())
            .body(bodyAsString(sdkRequest))
            .build();

    // TODO: cache MRU signatures?
    SigningResponse response = newRequest().post(request).readEntity(SigningResponse.class);

    SdkHttpFullRequest.Builder result = sdkRequest.toBuilder();
    result.encodedPath(""); // reset previous encoded path
    result.uri(response.uri()); // combines new URI with the old (now empty) encoded path
    result.headers(response.headers());

    return result.build();
  }

  @Override
  protected void processRequestPayload(
      SdkHttpFullRequest.Builder mutableRequest,
      byte[] signature,
      byte[] signingKey,
      Aws4SignerRequestParams signerRequestParams,
      AwsS3V4SignerParams signerParams) {
    if (signerParams.enablePayloadSigning()) {
      throw new UnsupportedOperationException("Payload signing is not supported");
    }
  }

  @Override
  protected void processRequestPayload(
      SdkHttpFullRequest.Builder mutableRequest,
      byte[] signature,
      byte[] signingKey,
      Aws4SignerRequestParams signerRequestParams,
      AwsS3V4SignerParams signerParams,
      SdkChecksum sdkChecksum) {
    processRequestPayload(mutableRequest, signature, signingKey, signerRequestParams, signerParams);
  }

  @Override
  protected String calculateContentHashPresign(
      SdkHttpFullRequest.Builder builder, Aws4PresignerParams aws4PresignerParams) {
    // Apparently this method is called only for pre-signed URLs, which are not supported (see
    // presign() below).
    throw new UnsupportedOperationException();
  }

  @Override
  public SdkHttpFullRequest presign(
      SdkHttpFullRequest sdkHttpFullRequest, ExecutionAttributes executionAttributes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  private String bodyAsString(SdkHttpFullRequest request) {
    if (isDeleteObjectsRequest(request) && request.contentStreamProvider().isPresent()) {
      try (InputStream is = request.contentStreamProvider().get().newStream()) {
        return IoUtils.toUtf8String(is);
      } catch (IOException ignored) {
      }
    }
    return null;
  }

  private boolean isDeleteObjectsRequest(SdkHttpFullRequest request) {
    return request.method() == SdkHttpMethod.POST
        && request.rawQueryParameters().containsKey("delete");
  }
}
