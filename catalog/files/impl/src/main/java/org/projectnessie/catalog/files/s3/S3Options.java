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
import java.util.Map;
import java.util.Optional;

public interface S3Options<PER_BUCKET extends S3BucketOptions> extends S3BucketOptions {
  /**
   * The default type of cloud to use, if not configured {@linkplain #buckets() per bucket}. The
   * cloud type must be configured, either {@linkplain #buckets() per bucket} or here.
   */
  @Override
  Optional<Cloud> cloud();

  /**
   * The default endpoint override to use, if not configured {@linkplain #buckets() per bucket}. The
   * endpoint must be specified for {@linkplain Cloud#PRIVATE private clouds}, either {@linkplain
   * #buckets() per bucket} or here.
   */
  @Override
  Optional<URI> endpoint();

  /**
   * Whether to use path-style access. If true, path-style access will be used, as in: {@code
   * https://<domain>/<bucket>}. If false, a virtual-hosted style will be used instead, as in:
   * {@code https://<bucket>.<domain>}. If unspecified, the default will depend on the cloud
   * provider.
   */
  @Override
  Optional<Boolean> pathStyleAccess();

  /**
   * The default DNS name of the region to use, if not configured {@linkplain #buckets() per
   * bucket}. The region must be specified for {@linkplain Cloud#AMAZON AWS}, either {@linkplain
   * #buckets() per bucket} or here.
   */
  @Override
  Optional<String> region();

  /**
   * The default project ID to use, if not configured {@linkplain #buckets() per bucket}, for
   * example for {@linkplain Cloud#GOOGLE Google cloud}.
   */
  @Override
  Optional<String> projectId();

  /**
   * The default reference to the access-key-id to use, if not configured {@linkplain #buckets() per
   * bucket}. An access-key-id must be configured, either {@linkplain #buckets() per bucket} or
   * here.
   */
  @Override
  Optional<String> accessKeyIdRef();

  /**
   * The default reference to the secret-access-key to use, if not configured {@linkplain #buckets()
   * per bucket}. A secret-access-key must be configured, either {@linkplain #buckets() per bucket}
   * or here.
   */
  @Override
  Optional<String> secretAccessKeyRef();

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the values from {@link S3Options}.
   */
  Map<String, PER_BUCKET> buckets();

  default S3BucketOptions effectiveOptionsForBucket(Optional<String> bucketName) {
    if (bucketName.isEmpty()) {
      return this;
    }
    S3BucketOptions perBucket = buckets().get(bucketName.get());
    if (perBucket == null) {
      return this;
    }

    return S3ProgrammaticOptions.S3PerBucketOptions.builder().from(this).from(perBucket).build();
  }
}
