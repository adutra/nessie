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
package org.projectnessie.tools.contentgenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.tools.contentgenerator.ITReadCommits.assertOutputContains;
import static org.projectnessie.tools.contentgenerator.ITReadCommits.assertOutputDoesNotContain;
import static org.projectnessie.tools.contentgenerator.RunContentGenerator.runGeneratorCmd;

import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Operation;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;

class ITReadContent extends AbstractContentGeneratorTest {

  private Branch branch;

  @BeforeEach
  void setup() throws NessieConflictException, NessieNotFoundException {
    try (NessieApiV2 api = buildNessieApi()) {
      branch = makeCommit(api);
    }
  }

  @Test
  void readContent() {

    ProcessResult proc =
        runGeneratorCmd(
            "content",
            "--uri",
            NESSIE_API_URI,
            "--ref",
            branch.getName(),
            "--key",
            CONTENT_KEY.getElements().get(0),
            "--key",
            CONTENT_KEY.getElements().get(1));
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();

    assertOutputContains(
        output,
        "Reading content for key '"
            + CONTENT_KEY
            + "' on reference '"
            + branch.getName()
            + "' @ HEAD...",
        "Done reading content for key '"
            + CONTENT_KEY
            + "' on reference '"
            + branch.getName()
            + "' @ HEAD.");
  }

  @Test
  void readContentVerbose() {
    ProcessResult proc =
        runGeneratorCmd(
            "content",
            "--uri",
            NESSIE_API_URI,
            "--ref",
            branch.getName(),
            "--verbose",
            "--key",
            CONTENT_KEY.getElements().get(0),
            "--key",
            CONTENT_KEY.getElements().get(1));
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();

    assertOutputContains(
        output,
        "Reading content for key '"
            + CONTENT_KEY
            + "' on reference '"
            + branch.getName()
            + "' @ HEAD...",
        "key[0]: " + CONTENT_KEY.getElements().get(0),
        "key[1]: " + CONTENT_KEY.getElements().get(1),
        "Done reading content for key '"
            + CONTENT_KEY
            + "' on reference '"
            + branch.getName()
            + "' @ HEAD.");
  }

  @ParameterizedTest
  @CsvSource(
      value = {"%1$s|true", "%2$s|false", "%2$s~1|true"},
      delimiter = '|')
  void readContentsWithHash(String hashTemplate, boolean expectContent) throws Exception {

    String c1 = branch.getHash();
    try (NessieApiV2 api = buildNessieApi()) {
      branch =
          api.commitMultipleOperations()
              .branchName(branch.getName())
              .hash(Objects.requireNonNull(c1))
              .commitMeta(CommitMeta.fromMessage("Second commit"))
              .operation(Operation.Delete.of(CONTENT_KEY))
              .commit();
    }
    String c2 = branch.getHash();

    String hash = String.format(hashTemplate, c1, c2);
    ProcessResult proc =
        runGeneratorCmd(
            "content",
            "--uri",
            NESSIE_API_URI,
            "--ref",
            branch.getName(),
            "--hash",
            hash,
            "--key",
            CONTENT_KEY.getElements().get(0),
            "--key",
            CONTENT_KEY.getElements().get(1));
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();

    assertOutputContains(
        output,
        "Reading content for key '"
            + CONTENT_KEY
            + "' on reference '"
            + branch.getName()
            + "' @ "
            + hash,
        "Done reading content for key '"
            + CONTENT_KEY
            + "' on reference '"
            + branch.getName()
            + "' @ "
            + hash);

    if (expectContent) {
      assertOutputContains(output, "Key: " + CONTENT_KEY, "Value: IcebergTable");
    } else {
      assertOutputDoesNotContain(output, "Key: " + CONTENT_KEY, "Value: IcebergTable");
    }
  }
}
