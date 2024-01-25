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

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.files.api.BackendThrottledException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(SoftAssertionsExtension.class)
public class TestS3ObjectIO {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void readObjectThrottledThrowsBackendThrottledException() {
    S3Client s3client = mock(S3Client.class);

    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
    Duration defaultRetryAfter = Duration.of(10, SECONDS);

    when(s3client.utilities())
        .thenReturn(S3Utilities.builder().region(Region.EU_CENTRAL_1).build());

    when(s3client.getObject(any(GetObjectRequest.class)))
        .thenThrow(S3Exception.builder().statusCode(429).message("blah").build());

    S3ObjectIO objectIO = new S3ObjectIO(() -> s3client, clock, defaultRetryAfter);

    soft.assertThatThrownBy(() -> objectIO.readObject(URI.create("s3://hello/foo/bar")))
        .isInstanceOf(BackendThrottledException.class)
        .asInstanceOf(type(BackendThrottledException.class))
        .extracting(BackendThrottledException::retryNotBefore)
        .isEqualTo(now.plus(defaultRetryAfter));
  }
}
