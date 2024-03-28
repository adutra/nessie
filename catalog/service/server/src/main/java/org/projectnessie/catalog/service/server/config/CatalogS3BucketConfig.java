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
package org.projectnessie.catalog.service.server.config;

import io.smallrye.config.WithName;
import java.net.URI;
import java.util.Optional;
import org.projectnessie.catalog.files.s3.Cloud;
import org.projectnessie.catalog.files.s3.S3BucketOptions;

public interface CatalogS3BucketConfig extends S3BucketOptions {

  @WithName("endpoint")
  @Override
  Optional<URI> endpoint();

  @WithName("path-style-access")
  @Override
  Optional<Boolean> pathStyleAccess();

  @WithName("cloud")
  @Override
  Optional<Cloud> cloud();

  @WithName("region")
  @Override
  Optional<String> region();

  @WithName("project-id")
  @Override
  Optional<String> projectId();

  @WithName("access-key-id-ref")
  @Override
  Optional<String> accessKeyIdRef();

  @WithName("secret-access-key-ref")
  @Override
  Optional<String> secretAccessKeyRef();

  @WithName("access-point")
  @Override
  Optional<String> accessPoint();

  @WithName("allow-cross-region-access-point")
  @Override
  Optional<Boolean> allowCrossRegionAccessPoint();

  @WithName("sts.endpoint")
  @Override
  Optional<URI> stsEndpoint();

  @WithName("assumed-role")
  @Override
  Optional<String> roleArn();

  @WithName("session-iam-policy")
  @Override
  Optional<String> iamPolicy();

  @WithName("role-session-name")
  @Override
  Optional<String> roleSessionName();

  @WithName("external-id")
  @Override
  Optional<String> externalId();
}
