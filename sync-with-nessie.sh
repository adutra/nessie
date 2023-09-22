#!/bin/bash
#
# Script to manually sync projectnessie/nessie/main into the `catalog` branch.
#
# This script RESETS local `catalog` branch to the remote branch.

set -e

[[ -z $(git remote | grep -w projectnessie) ]] && git remote add projectnessie https://github.com/projectnessie/nessie.git
[[ -z $(git remote | grep -w catalog-private) ]] && git remote add catalog-private https://github.com/projectnessie/nessie-catalog-private.git

if [[ -n $(git status --porcelain) ]] ; then
  echo "Worktree not clean! Exiting!"
  exit 1
fi

PREV_BRANCH=""

function reset_branch() {
  echo ""
  echo "Switching bach to branch ${PREV_BRANCH}"
  echo "---------------------------------------"
  git checkout ${PREV_BRANCH}
}

echo ""
echo "Fetching 'main' from projectnessie..."
echo "-------------------------------------"
git fetch projectnessie main
echo ""
echo "Fetching 'catalog' from catalog-private..."
echo "------------------------------------------"
git fetch catalog-private catalog

PREV_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
trap reset_branch EXIT

git checkout -B catalog-nessie-sync catalog-private/catalog

echo ""
echo "Merging 'main' into 'catalog'..."
echo "--------------------------------"
PREV_HASH="$(git rev-parse HEAD)"
if ! git merge projectnessie/main ; then
  echo ""
  echo ""
  echo ""
  echo "!!! Merge of 'main' into 'catalog' failed, fix merge conflicts and then:"
  echo "  git commit"
  echo "  git push catalog-private HEAD:catalog"
  echo ""
  exit 1
fi
NEW_HASH="$(git rev-parse HEAD)"

if [[ ${PREV_HASH} != ${NEW_HASH} ]] ; then
  echo ""
  echo "Branch 'catalog' updated, pushing to catalog-private..."
  echo "-------------------------------------------------------"
  git push catalog-private HEAD:catalog
else
  echo ""
  echo "No changes from nessie/main."
  echo ""
fi

