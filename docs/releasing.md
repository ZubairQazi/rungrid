# Releasing

1. Run database/race and Compose fault suites from a clean checkout.
2. Run HeteroSplit and archive verified metrics; document grid/data/epoch differences.
3. Run scaling/recovery benchmarks and save raw evidence with its revision.
4. Commit evidence and create an annotated `v0.1.0` tag when ready.
5. Run `python scripts/release.py` for Linux/macOS amd64/arm64 archives and SHA-256
   checksums. `GO=/path/to/go` overrides the tool. Archives use fixed timestamps.
6. Push repository/tag to the intended GitHub remote. Check actual CI and release
   workflow outcomes; the workflow publishes binaries on tag pushes.

A local tag and archives do not prove hosted CI passed or a public release exists.
The evidence document distinguishes local from hosted verification.
