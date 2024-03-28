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

import java.net.URI;
import java.util.Optional;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Utilities;

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

  /**
   * Whether to use path-style access. If true, path-style access will be used, as in: {@code
   * https://<domain>/<bucket>}. If false, a virtual-hosted style will be used instead, as in:
   * {@code https://<bucket>.<domain>}. If unspecified, the default will depend on the cloud
   * provider.
   */
  Optional<Boolean> pathStyleAccess();

  /**
   * AWS Access point for this bucket. Access points can be used to perform S3 operations by
   * specifying a mapping of bucket to access points. This is useful for multi-region access,
   * cross-region access, disaster recovery, etc.
   *
   * @see <a
   *     href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Access
   *     Points</a>
   */
  Optional<String> accessPoint();

  /**
   * Authorize cross-region calls when contacting an {@link #accessPoint()}.
   *
   * <p>By default, attempting to use an access point in a different region will throw an exception.
   * When enabled, this property allows using access points in other regions.
   */
  Optional<Boolean> allowCrossRegionAccessPoint();

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

  /**
   * Extract the bucket name from the URI. Only relevant for {@linkplain Cloud#AMAZON AWS} or
   * {@linkplain Cloud#PRIVATE S3-compatible private} clouds; the behavior of this method is
   * unspecified for other clouds.
   *
   * @param uri URI to extract the bucket name from; both s3 and https schemes are accepted, and
   *     https schemes can be either path-style or virtual-host-style.
   */
  default Optional<String> extractBucket(URI uri) {
    return S3Utilities.builder()
        // we don't care about the region here
        .region(Region.US_EAST_1)
        .build()
        .parseUri(uri)
        .bucket();
  }
}
