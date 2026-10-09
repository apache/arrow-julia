# Issue #540 Triage Report

## Summary
Files written by `polars.DataFrame.write_ipc` with `string_view` type are read incorrectly by Arrow.jl.

## Issue Type
Bug (FIXED)

## Root Cause
Commit `de59e48` (Fix bug where inline bytes weren't valid for Utf8View data) fixed this issue.

The bug was in `src/table.jl` in the `build` function for ViewTypes:

**Before (buggy):**
```julia
inline, views = reinterp(ViewElement, batch, buffer, rb.compression)
```

**After (fixed):**
```julia
_, views = reinterp(ViewElement, batch, buffer, rb.compression)
inline = reinterpret(UInt8, views)  # reuse the (possibly realigned) memory backing `views`
```

The `reinterp` function returns `(bytes, array)` where:
- `bytes`: the raw byte buffer (original or decompressed)
- `array`: the typed array (ViewElement in this case)

The bug assigned `bytes` to `inline`, which was wrong. The inline data should be a byte view of the `views` array itself (since inline string data is stored within the ViewElement structs).

## Resolution
- Fixed in commit `de59e48` (May 9, 2025)
- Included in releases: v2.8.1-rc1, v2.8.1-rc2
- Will be in v2.8.1 final release

## Recommendation
**Close issue #540** with a note that the fix is included in v2.8.1.

## Test Coverage
- Test file `test/reject_reason_trimmed.arrow` added with the fix
- Test verifies Utf8View data is read correctly
