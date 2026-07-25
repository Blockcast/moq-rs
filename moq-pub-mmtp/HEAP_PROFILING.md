<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Heap profiling image

This diagnostic build keeps the publisher CLI and protocol path unchanged. It
selects jemalloc only for `moq-pub-mmtp` and exposes retained allocation stacks
plus allocator totals on the existing private profiling listener. The default
build does not include jemalloc and is unchanged. The diagnostic binary is
linked as non-PIE with frame pointers so `jeprof` can symbolize sampled
addresses without reconstructing ASLR relocation offsets.

## Build

Build the profiling branch, which descends directly from PR #47 commit
`ec22a944e656163ef41fcd6de77199f28a34f15c`:

```sh
git merge-base --is-ancestor \
  ec22a944e656163ef41fcd6de77199f28a34f15c HEAD
docker build --file Dockerfile \
  --build-arg HEAP_PROFILING=1 \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  --build-arg BASE_REVISION=ec22a944e656163ef41fcd6de77199f28a34f15c \
  --build-arg PROFILE_KIND=heap \
  --tag moq-pub-mmtp:pr47-heap-profile .
docker image inspect --format '{{.Id}}' moq-pub-mmtp:pr47-heap-profile
```

The `SOURCE_REVISION` label records the profiling patch, while
`BASE_REVISION` records the behavior-bearing PR #47 commit. The image ID is the
immutable local content digest; use the full registry digest after pushing.

## Run

The profiling allocator is compiled with sampling dormant. Profiling remains
disabled unless `MOQ_PUB_PROFILE_ADDR` is explicitly supplied; the endpoint
then activates allocation sampling at one sample per roughly 512 KiB. Keep the
listener on loopback and use the same publisher arguments/config as the PR #47
image:

```sh
docker run --rm --memory=512m \
  -e MOQ_PUB_PROFILE_ADDR=127.0.0.1:6060 \
  moq-pub-mmtp:pr47-heap-profile \
  moq-pub-mmtp <existing PR #47 publisher arguments>
```

## Capture

For Kubernetes, port-forward the loopback-only listener first:

```sh
kubectl port-forward pod/<publisher-pod> 6060:6060
curl --fail http://127.0.0.1:6060/debug/pprof/heap -o start.heap
curl --fail http://127.0.0.1:6060/debug/allocator -o start-allocator.json
# Repeat as mid.heap / end.heap and mid-allocator.json / end-allocator.json.
```

The JSON fields distinguish application live bytes (`allocated_bytes`), active
allocator pages (`active_bytes`), reusable bytes within active pages
(`reusable_active_bytes`), physical allocator residency (`resident_bytes`),
and retained virtual mappings (`retained_virtual_bytes`).

## Analyze

Install the matching jemalloc `jeprof` script and point it at the unstripped
binary from the same image build:

```sh
docker create --name moq-profile-bin moq-pub-mmtp:pr47-heap-profile
docker cp moq-profile-bin:/usr/local/bin/moq-pub-mmtp ./moq-pub-mmtp
docker rm moq-profile-bin
jeprof --show_bytes --text ./moq-pub-mmtp start.heap > start.txt
jeprof --show_bytes --text ./moq-pub-mmtp mid.heap > mid.txt
jeprof --show_bytes --text ./moq-pub-mmtp end.heap > end.txt
jeprof --show_bytes --svg ./moq-pub-mmtp end.heap > end.svg
```

Compare the retained stack/type totals in the three text reports with the
allocator JSON snapshots. Do not deploy this image to production.
