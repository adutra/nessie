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
package org.projectnessie.tools.admin.cli;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.versioned.storage.common.persist.Persist;

@QuarkusMainTest
@TestProfile(BaseConfigProfile.class)
@ExtendWith(NessieServerAdminTestExtension.class)
class ITCheckContentPersist extends BaseContentPersistTest<CheckContentEntry> {

  ITCheckContentPersist(Persist persist) {
    super(persist, CheckContentEntry.class);
  }

  @Nested
  class PersistCheckContentTests extends AbstractCheckContentTests {

    PersistCheckContentTests() {
      super(ITCheckContentPersist.this);
    }
  }
}
