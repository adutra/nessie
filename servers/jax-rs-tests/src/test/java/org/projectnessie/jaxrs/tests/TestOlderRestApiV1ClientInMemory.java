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
package org.projectnessie.jaxrs.tests;

import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtension;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientResponseFactory;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemorytests.InmemoryBackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

/**
 * This tests runs the usual API v1 tests, but checks that JSON responses do not have API v2
 * attributes.
 */
@ExtendWith(PersistExtension.class)
@NessieBackend(InmemoryBackendTestFactory.class)
@NessieApiVersions(versions = NessieApiVersion.V1)
@NessieClientResponseFactory(ValidatingApiV1ResponseFactory.class)
class TestOlderRestApiV1ClientInMemory extends BaseTestNessieApi {

  @NessiePersist static Persist persist;

  @RegisterExtension static NessieJaxRsExtension server = jaxRsExtension(() -> persist);
}
