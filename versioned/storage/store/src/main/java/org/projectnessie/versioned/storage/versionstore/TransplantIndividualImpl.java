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
package org.projectnessie.versioned.storage.versionstore;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove.commitRemove;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceNotFound;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.fromCommitMeta;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.toCommitMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.ResultType;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.TransplantOp;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

final class TransplantIndividualImpl extends BaseCommitHelper implements Transplant {

  TransplantIndividualImpl(
      @Nonnull @jakarta.annotation.Nonnull BranchName branch,
      @Nonnull @jakarta.annotation.Nonnull Optional<Hash> referenceHash,
      @Nonnull @jakarta.annotation.Nonnull Persist persist,
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      @Nullable @jakarta.annotation.Nullable CommitObj head)
      throws ReferenceNotFoundException {
    super(branch, referenceHash, persist, reference, head);
  }

  @Override
  public MergeResult<Commit> transplant(Optional<?> retryState, TransplantOp transplantOp)
      throws ReferenceNotFoundException, RetryException, ReferenceConflictException {
    MergeTransplantContext mergeTransplantContext = loadSourceCommitsForTransplant(transplantOp);

    ImmutableMergeResult.Builder<Commit> mergeResult =
        prepareMergeResult().resultType(ResultType.TRANSPLANT).sourceRef(transplantOp.fromRef());

    IndexesLogic indexesLogic = indexesLogic(persist);
    StoreIndex<CommitOp> sourceParentIndex =
        indexesLogic.buildCompleteIndexOrEmpty(mergeTransplantContext.baseCommit());
    StoreIndex<CommitOp> targetParentIndex = indexesLogic.buildCompleteIndexOrEmpty(head);

    MergeBehaviors mergeBehaviors = new MergeBehaviors(transplantOp);

    CommitLogic commitLogic = commitLogic(persist);
    ObjId newHead = headId();
    boolean empty = true;
    Map<ContentKey, MergeResult.KeyDetails> keyDetailsMap = new HashMap<>();
    for (CommitObj sourceCommit : mergeTransplantContext.sourceCommits()) {
      CreateCommit createCommit =
          cloneCommit(
              transplantOp.updateCommitMetadata(), sourceCommit, sourceParentIndex, newHead);

      validateMergeTransplantCommit(createCommit, transplantOp.validator(), targetParentIndex);

      verifyMergeTransplantCommitPolicies(targetParentIndex, sourceCommit);

      List<Obj> objsToStore = new ArrayList<>();
      CommitObj newCommit =
          createMergeTransplantCommit(
              mergeBehaviors, keyDetailsMap, createCommit, objsToStore::add);

      if (!indexesLogic.commitOperations(newCommit).iterator().hasNext()) {
        // No operations in this commit, skip it.
        continue;
      }

      empty = false;
      if (!transplantOp.dryRun()) {
        newHead = newCommit.id();
        commitLogic.storeCommit(newCommit, objsToStore);
        mergeResult.addCreatedCommits(commitObjToCommit(newCommit));
      }

      sourceParentIndex = indexesLogic.buildCompleteIndex(sourceCommit, Optional.empty());
      targetParentIndex = indexesLogic.buildCompleteIndex(newCommit, Optional.empty());
    }

    boolean hasConflicts = recordKeyDetailsAndCheckConflicts(mergeResult, keyDetailsMap);

    return finishMergeTransplant(empty, mergeResult, newHead, transplantOp.dryRun(), hasConflicts);
  }

  private CreateCommit cloneCommit(
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      CommitObj sourceCommit,
      StoreIndex<CommitOp> sourceParentIndex,
      ObjId newHead) {
    CreateCommit.Builder createCommitBuilder = newCommitBuilder().parentCommitId(newHead);

    CommitMeta commitMeta = toCommitMeta(sourceCommit);
    CommitMeta updatedMeta = updateCommitMetadata.rewriteSingle(commitMeta);
    fromCommitMeta(updatedMeta, createCommitBuilder);

    IndexesLogic indexesLogic = indexesLogic(persist);
    for (StoreIndexElement<CommitOp> el : indexesLogic.commitOperations(sourceCommit)) {
      StoreIndexElement<CommitOp> expected = sourceParentIndex.get(el.key());
      ObjId expectedId = null;
      if (expected != null) {
        CommitOp expectedContent = expected.content();
        if (expectedContent.action().exists()) {
          expectedId = expectedContent.value();
        }
      }

      CommitOp op = el.content();
      if (op.action().exists()) {
        createCommitBuilder.addAdds(
            commitAdd(
                el.key(), op.payload(), requireNonNull(op.value()), expectedId, op.contentId()));
      } else {
        createCommitBuilder.addRemoves(
            commitRemove(el.key(), op.payload(), requireNonNull(expectedId), op.contentId()));
      }
    }

    return createCommitBuilder.build();
  }

  MergeTransplantContext loadSourceCommitsForTransplant(VersionStore.TransplantOp transplantOp)
      throws ReferenceNotFoundException {
    List<Hash> commitHashes = transplantOp.sequenceToTransplant();

    checkArgument(
        !commitHashes.isEmpty(),
        "No hashes to transplant onto %s @ %s, expected commit ID from request was %s.",
        head != null ? head.id() : EMPTY_OBJ_ID,
        branch.getName(),
        referenceHash.map(Hash::asString).orElse("not specified"));

    Obj[] objs;
    try {
      objs =
          persist.fetchObjs(
              commitHashes.stream().map(TypeMapping::hashToObjId).toArray(ObjId[]::new));
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(e);
    }
    List<CommitObj> commits = new ArrayList<>(commitHashes.size());
    CommitObj parent = null;
    CommitLogic commitLogic = commitLogic(persist);
    for (int i = 0; i < objs.length; i++) {
      Obj o = objs[i];
      if (o == null) {
        throw RefMapping.hashNotFound(commitHashes.get(i));
      }
      CommitObj commit = (CommitObj) o;
      if (i > 0) {
        if (!commit.directParent().equals(commits.get(i - 1).id())) {
          throw new IllegalArgumentException("Sequence of hashes is not contiguous.");
        }
      } else {
        try {
          parent = commitLogic.fetchCommit(commit.directParent());
        } catch (ObjNotFoundException e) {
          throw referenceNotFound(e);
        }
      }
      commits.add(commit);
    }

    List<CommitMeta> commitsMetadata = new ArrayList<>(commits.size());
    for (CommitObj sourceCommit : commits) {
      commitsMetadata.add(toCommitMeta(sourceCommit));
    }
    CommitMeta metadata =
        transplantOp.updateCommitMetadata().squash(commitsMetadata, commits.size());

    return new MergeTransplantContext(commits, parent, metadata);
  }
}
