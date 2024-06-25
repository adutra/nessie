#!/usr/bin/env bash
#
# Copyright (C) 2024 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Helper script to create the release notes for GitHub

set -e

NOTES_FILE=""
LAST_TAG=""
GIT_TAG=""
RELEASE_VERSION=""
CHANGELOG_FILE=""

function usage() {
  cat << ! > /dev/stderr
  Usage: $0 -n NOTES_FILE -l LAST_TAG -t GIT_TAG -r RELEASE_VERSION -c CHANGELOG_FILE

    NOTES_FILE        output file
    LAST_TAG          previously released Nessie Git tag
    GIT_TAG           current release Git tag
    RELEASE_VERSION   Nessie version being released
    CHANGELOG_FILE    Changelog file, generated by Gradle, example:
                        ./gradlew --no-scan --quiet --console=plain getChangelog --no-header --no-links \
                          > ./nessie-changelog-${RELEASE_VERSION}.md
!
}

while [[ $# -gt 0 ]]; do
  arg="$1"
  case "$arg" in
  -n)
    NOTES_FILE="$2"
    shift
    ;;
  -l)
    LAST_TAG="$2"
    shift
    ;;
  -t)
    GIT_TAG="$2"
    shift
    ;;
  -r)
    RELEASE_VERSION="$2"
    shift
    ;;
  -c)
    CHANGELOG_FILE="$2"
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    usage
    exit 1
    ;;
  esac
  shift
done

if [[ -z ${NOTES_FILE} || -z ${LAST_TAG} || -z ${GIT_TAG} || -z ${RELEASE_VERSION} || -z ${CHANGELOG_FILE} ]] ; then
  usage
  exit 1
fi

# xargs trims
NUM_COMMITS=$(git log --format='format:%h' ${LAST_TAG}..HEAD^1 | wc -l | xargs)

git log --perl-regexp --author '^(?!.*renovate|.*nessie-release-workflow).*$' --format='format:* %s' ${LAST_TAG}..${GIT_TAG} | grep -v '^\* \[release\] .*$' > ./release-log

Q_CLI_URL="https://github.com/projectnessie/nessie/releases/download/nessie-${RELEASE_VERSION}/nessie-cli-${RELEASE_VERSION}.jar"
Q_GC_TOOL_URL="https://github.com/projectnessie/nessie/releases/download/nessie-${RELEASE_VERSION}/nessie-gc-${RELEASE_VERSION}.jar"
Q_UBER_URL="https://github.com/projectnessie/nessie/releases/download/nessie-${RELEASE_VERSION}/nessie-quarkus-${RELEASE_VERSION}-runner.jar"
Q_SERVER_ADMIN_URL="https://github.com/projectnessie/nessie/releases/download/nessie-${RELEASE_VERSION}/nessie-server-admin-tool-${RELEASE_VERSION}-runner.jar"
Q_HELM_CHART_URL="https://github.com/projectnessie/nessie/releases/download/nessie-${RELEASE_VERSION}/nessie-helm-${RELEASE_VERSION}.tgz"
Q_MC_URL="https://search.maven.org/search?q=g:org.projectnessie+AND+a:nessie-quarkus+AND+v:${RELEASE_VERSION}"

cat <<EOF > ${NOTES_FILE}
* ${NUM_COMMITS} commits since ${LAST_TAG#nessie-}
* Maven Central: https://search.maven.org/search?q=g:org.projectnessie.nessie+v:${RELEASE_VERSION}
* Docker images: https://github.com/projectnessie/nessie/pkgs/container/nessie and https://quay.io/repository/projectnessie/nessie?tab=tags
  It is a multiplatform Java image (amd64, arm64, ppc64le, s390x): \`docker pull ghcr.io/projectnessie/nessie:${RELEASE_VERSION}-java\`
* PyPI: https://pypi.org/project/pynessie/ (See [pynessie](https://github.com/projectnessie/pynessie/releases))
* Helm Chart repo: https://charts.projectnessie.org/

## Try it

The attached [\`nessie-quarkus-${RELEASE_VERSION}-runner.jar\`](${Q_UBER_URL}) is a standalone uber-jar file that runs on Java 17 or newer and it is also available via [Maven Central](${Q_MC_URL}). Download and run it (requires Java 17):
\`\`\`
wget ${Q_UBER_URL}
java -jar nessie-quarkus-${RELEASE_VERSION}-runner.jar
\`\`\`

Nessie CLI is attached as [\`nessie-cli-${RELEASE_VERSION}.jar\`](${Q_CLI_URL}), which is a standalone uber-jar file that runs on Java 11 or newer. Nessie CLI is also available as a Docker image: \`docker run --rm -it ghcr.io/projectnessie/nessie-cli:${RELEASE_VERSION}\`.

Nessie GC tool is attached as [\`nessie-gc-${RELEASE_VERSION}.jar\`](${Q_GC_TOOL_URL}), which is a standalone uber-jar file that runs on Java 11 or newer. Shell completion can be generated from the tool, check its \`help\` command. Nessie GC tool is also available as a Docker image: \`docker run --rm ghcr.io/projectnessie/nessie-gc:${RELEASE_VERSION} --help\`.

Nessie Server Admin tool is attached as [\`nessie-server-admin-tool-${RELEASE_VERSION}-runner.jar\`](${Q_SERVER_ADMIN_URL}), which is a standalone uber-jar file that runs on Java 17 or newer. Shell completion can be generated from the tool, check its \`help\` command. Nessie Server Admin tool is also available as a Docker image: \`docker run --rm ghcr.io/projectnessie/nessie-server-admin:${RELEASE_VERSION} --help\`.

The attached [\`nessie-helm-${RELEASE_VERSION}.tgz\`](${Q_HELM_CHART_URL}) is a packaged Helm chart, which can be downloaded and installed via Helm. There is also the [Nessie Helm chart repo](https://charts.projectnessie.org/), which can be added and used to install the Nessie Helm chart.

## Changelog

$(cat ${CHANGELOG_FILE})

## Full Changelog (minus renovate commits):

$(cat ./release-log)
EOF

rm ./release-log
