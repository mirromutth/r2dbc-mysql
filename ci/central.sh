#!/bin/bash
#
# Copyright 2018-2021 the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

VERSION=$(./mvnw org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -o | grep -v INFO)

if [[ $VERSION =~ [^(\d+\.){3}(RC(\d+)|M(\d+)|RELEASE)$] ]]; then
  echo "Staging $VERSION to Maven Central"
  ./mvnw -B clean deploy -Pcentral -s ./settings.xml -DskipITs -Dgpg.passphrase=${GPG_PASSPHRASE}
elif [[ $VERSION =~ [^.*-SNAPSHOT$] ]]; then
  echo "Cannot deploy a snapshot to Maven Central: $VERSION"
  exit 1
else
  echo "Not a correct version: $VERSION"
  exit 1
fi
