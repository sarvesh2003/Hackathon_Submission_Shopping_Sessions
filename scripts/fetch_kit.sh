#!/usr/bin/env bash
# Downloads the official generator and dev answer into kit/ (git-ignored) and builds kit/bin/generate.
set -euo pipefail
cd "$(dirname "$0")/.."
BASE=https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files
mkdir -p kit/bin
for f in generator.zip dev-answer.zip; do
  [ -s "kit/$f" ] || curl -fsSL -o "kit/$f" "$BASE/$f"
done
rm -rf kit/generator kit/dev-answer
unzip -q -o kit/generator.zip -d kit/      # -> kit/generator/
unzip -q -o kit/dev-answer.zip -d kit/     # -> kit/dev-answer/{user_daily,campaign_daily}.csv
# The generator has no dependencies; --offline avoids any network during the build.
cargo build --release --offline --manifest-path kit/generator/Cargo.toml --target-dir kit/generator-target
cp kit/generator-target/release/generate kit/bin/generate
echo "ok: kit/bin/generate and kit/dev-answer/ are ready"
