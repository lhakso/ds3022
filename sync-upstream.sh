#!/usr/bin/env bash
# Sync local main with the original repository. Optionally update your fork.
set -euo pipefail

push=false
case "${1:-}" in
  "") ;;
  --push) push=true ;;
  -h|--help)
    echo "Usage: $0 [--push]"
    echo "Fetch upstream/main and merge it into local main."
    echo "Add --push to also push main to origin."
    exit 0
    ;;
  *) echo "Usage: $0 [--push]" >&2; exit 1 ;;
esac
if (( $# > 1 )); then
  echo "Usage: $0 [--push]" >&2
  exit 1
fi

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

if [[ "$(git symbolic-ref --quiet --short HEAD || true)" != main ]]; then
  echo "Switch to main first: git switch main" >&2
  exit 1
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Commit or stash your tracked changes before syncing." >&2
  exit 1
fi

if ! git remote get-url upstream >/dev/null 2>&1; then
  echo "Missing upstream remote. Add it with:" >&2
  echo "  git remote add upstream https://github.com/uvasds-systems/ds3022.git" >&2
  exit 1
fi

git fetch upstream refs/heads/main
if ! git merge --ff --no-edit FETCH_HEAD; then
  echo "Merge did not finish. Check git status." >&2
  echo "If there are conflicts, resolve them and commit, or run git merge --abort." >&2
  exit 1
fi

if [[ "$push" == true ]]; then
  git push origin main
else
  echo "Local main is synced. Run git push origin main to update your GitHub fork."
fi
