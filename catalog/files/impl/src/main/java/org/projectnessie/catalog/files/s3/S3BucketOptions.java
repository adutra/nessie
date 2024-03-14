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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

import java.net.URI;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface S3BucketOptions {
  /**
   * The type of cloud running the S3 service. The cloud type must be configured, either per bucket
   * or in {@link S3Options S3Options}.
   */
  Optional<Cloud> cloud();

  /**
   * Endpoint URI, required for {@linkplain Cloud#PRIVATE private clouds}. The endpoint must be
   * specified for {@linkplain Cloud#PRIVATE private clouds}, either per bucket or in {@link
   * S3Options S3Options}.
   */
  Optional<URI> endpoint();

  Optional<String> domain();

  /**
   * DNS name of the region, required for {@linkplain Cloud#AMAZON AWS}. The region must be
   * specified for {@linkplain Cloud#AMAZON AWS}, either per bucket or in {@link S3Options
   * S3Options}.
   */
  Optional<String> region();

  /** Project ID, if required, for example for {@linkplain Cloud#GOOGLE Google cloud}. */
  Optional<String> projectId();

  /**
   * Key used to look up the access-key-ID via {@link
   * org.projectnessie.catalog.files.secrets.SecretsProvider SecretsProvider}. An access-key-id must
   * be configured, either per bucket or in {@link S3Options S3Options}.
   */
  Optional<String> accessKeyIdRef();

  /**
   * Key used to look up the secret-access-key via {@link
   * org.projectnessie.catalog.files.secrets.SecretsProvider SecretsProvider}. A secret-access-key
   * must be configured, either per bucket or in {@link S3Options S3Options}.
   */
  Optional<String> secretAccessKeyRef();

  Pattern AWS_HOST_PATTERN = Pattern.compile("^(.*)[.]s3[.](.*)[.]amazonws[.]com$");

  default String extractBucket(URI uri) {
    Cloud cloud =
        cloud().orElseThrow(() -> new IllegalStateException("Must configure the cloud type"));
    switch (cloud) {
      case AMAZON:
        String host = uri.getHost();
        // Example: my-bucket.s3.us-east-1.amazonaws.com
        Matcher matcher = AWS_HOST_PATTERN.matcher(host);
        checkArgument(matcher.matches(), "%s is not an AWS S3 URL", host);
        return matcher.group(1);
      case GOOGLE:
        return "gcs"; // TODO there are no buckets in GCS, right?
      case PRIVATE:
        String path = uri.getPath();
        URI endpoint = resolveS3Endpoint();
        String basePath = endpoint.getPath();
        host = uri.getHost();
        if (domain().isPresent()) {
          String domain = domain().get();
          if (domain.equals(host)) {
            return null;
          }
          String dotDomain = "." + domain;
          checkArgument(
              host.endsWith(dotDomain) && path.startsWith(basePath),
              "%s is not resolvable against the configured S3 endpoint %s",
              uri,
              endpoint);
          return host.substring(0, host.length() - dotDomain.length());
        } else {
          checkArgument(
              host.equals(endpoint.getHost()) && path.startsWith(basePath),
              "%s is not resolvable against the configured S3 endpoint %s",
              uri,
              endpoint);
          int i = basePath.length();
          while (path.charAt(i) == '/') {
            i++;
          }
          String remaining = path.substring(i);
          i = remaining.indexOf('/');
          return i == -1 ? remaining : remaining.substring(0, i);
        }
      case MICROSOFT:
        throw new IllegalArgumentException("Cloud " + cloud + " not supported for S3");
      default:
        throw new IllegalArgumentException("Unknown cloud " + cloud);
    }
  }

  default URI resolveS3Endpoint() {
    Cloud cloud =
        cloud().orElseThrow(() -> new IllegalStateException("Must configure the cloud type"));
    switch (cloud) {
      case AMAZON:
        return endpoint()
            .orElseGet(
                () ->
                    URI.create(
                        format(
                            "https://s3.%s.amazonaws.com/",
                            region()
                                .orElseThrow(
                                    () ->
                                        new IllegalArgumentException(
                                            "Must configure the AWS region")))));
      case GOOGLE:
        return endpoint().orElseGet(() -> URI.create("https://storage.googleapis.com/"));
      case PRIVATE:
        return endpoint()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "No endpoint provided, but cloud "
                            + cloud
                            + " requires an explicit endpoint."));
      case MICROSOFT:
        throw new IllegalArgumentException("Cloud " + cloud + " not supported for S3");
      default:
        throw new IllegalArgumentException("Unknown cloud " + cloud);
    }
  }
}
