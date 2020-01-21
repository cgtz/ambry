#!/bin/sh

# exit when any command fails
set -e

echo "Building and testing artifacts, and creating pom files"
./gradlew -s --scan build intTest codeCoverageReport sonarqube publishToMavenLocal

#echo "Pull request: [$TRAVIS_PULL_REQUEST], Travis branch: [$TRAVIS_BRANCH]"
## release only from master when no pull request build
#if [ "$TRAVIS_BRANCH" = "master" ] && [ "$TRAVIS_PULL_REQUEST" = "false" ]
#then
#    echo "Releasing (tagging, uploading to Bintray)"
#    ./gradlew -s -i --scan ciPerformRelease
#fi
