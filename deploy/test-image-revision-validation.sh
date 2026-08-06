#!/bin/sh
# SPDX-FileCopyrightText: 2026 Blockcast Inc.
# SPDX-License-Identifier: MIT OR Apache-2.0

set -eu

expect_failure() {
	description=$1
	shift
	if "$@" >/dev/null 2>&1; then
		echo "error: expected $description to fail" >&2
		exit 1
	fi
}

revision=$(git rev-parse --verify HEAD)
export DOCKER_BUILDKIT=1
image="moq-rs:image-provenance-test-$$"
trap 'docker image rm --force "$image" >/dev/null 2>&1 || true' EXIT HUP INT TERM

expect_failure "missing revisions" \
	docker build --quiet --file Dockerfile --target image-provenance .
expect_failure "malformed SOURCE_REVISION" \
	docker build --quiet --file Dockerfile --target image-provenance \
	--build-arg SOURCE_REVISION=invalid \
	--build-arg BASE_REVISION="$revision" .
expect_failure "malformed BASE_REVISION" \
	docker build --quiet --file Dockerfile --target image-provenance \
	--build-arg SOURCE_REVISION="$revision" \
	--build-arg BASE_REVISION=invalid .

docker build --quiet --tag "$image" --file Dockerfile --target image-provenance \
	--build-arg SOURCE_REVISION="$revision" \
	--build-arg BASE_REVISION="$revision" . >/dev/null

test "$(docker image inspect "$image" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = "$revision"
test "$(docker image inspect "$image" --format '{{index .Config.Labels "org.opencontainers.image.base.revision"}}')" = "$revision"

echo "image provenance revision validation passed"
