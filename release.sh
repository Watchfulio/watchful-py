#!/bin/bash

# Watchful python library release script
#
# When executed, this script will first check for prerequisite tools required to
# do the release, suggesting a path to installation if they are not found. Next,
# it will check out a fresh version of watchful-py in a temporary directory--this
# means that the state of a working tree from where this script is called will never
# matter, and can be done mid-development branch. If the version was not specified,
# or was specified as one of major|minor|patch (from semver), the version is then
# calculated; a specific version may also be specified. After that, the version
# is bumped, a commit and tag are made _and pushed back up to main_, and then a
# Github release is generated with a changelog and attached to the tag.
#
# A CI job will run on tag push that will handle the build and upload to PyPI.

set -e

if [[ ! $(command -v hatch) ]]; then
    echo "Please install the hatch and re-run."
    echo
    echo "\`brew install hatch\` is the most direct installation path."
    exit 1
fi
if [[ ! $(command -v hub) ]]; then
    echo "Please install hub and re-run."
    echo
    echo "\`brew install hub\` should provide the tool. See the tool docs for configuration."
    exit 1
fi
if [[ ! $(command -v semver) ]]; then
    echo "Please install semver and re-run."
    echo
    echo "\`brew install ffurrer2/tap/semver\` is the most direct installation path."
    exit 1
fi

VERSION=$1
if [ "$VERSION" = "" ]; then
    VERSION=minor
    echo "No version specified. Assuming a minor release."
    echo
fi

TEMPDIR=`mktemp -d -t watchful-release`
trap 'rm -rf -- "$TEMPDIR"' EXIT

#echo "Using temporary directory: $TEMPDIR"
cd $TEMPDIR
git clone git@github.com:Watchfulio/watchful-py.git > /dev/null 2>&1
cd watchful-py

if [ "$(hub ci-status)" != "success" ]; then
    echo "The commit status is not in a clean state. Found: $(hub ci-status)"
    echo
    echo "Please address the build issues in HEAD and try again."
    exit 1
fi

case $VERSION in
    major | minor | patch)
        current=`hatch version`
        VERSION=`semver next ${VERSION} ${current}`
        ;;
    *)
        # Assuming the version was manually specified
        ;;
esac

hatch version $VERSION > /dev/null 2>&1
git add src/watchful/__about__.py
git commit -m "release: ${VERSION}" > /dev/null 2>&1
git tag -a -m "${VERSION}" "${VERSION}"
git push

previous_version=`git tag --sort=-creatordate | sed -n '2 p'`
# The tail step here ignores the commit that is the release, so we don't have a changelog that also
# contains, e.g. "release: 0.10.55". We already know it's a release, that's why we're constructing release
# notes.
commits=`git log --pretty=oneline ${previous_version}..${VERSION} | tail -n +2 | awk '{$1="-"; print }'`
hub release create $VERSION -m "Release ${VERSION}

${commits}"

echo "${VERSION} tagged and released"
