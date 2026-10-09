# Issue #420 Triage Report

## Summary
Test failure when running with >1 thread. Race condition causes `MethodError: no method matching resize!(::Arrow.DictEncoded{...}, ::Int64)`.

## Issue Type
Bug (Threading/Race Condition)

## Root Cause Analysis

The issue is related to dictionary encoding operations when reading Arrow data with multiple threads. The error occurs when:
1. Multiple partitions with dict-encoded columns are read
2. Threads try to append/combine dict-encoded arrays
3. Race conditions cause invalid operations on DictEncoded arrays

## Related Fixes

### Fix #535 (fc8b899) - Make DictEncoding reading threadsafe
Merged: Nov 26, 2024

This commit made DictEncoding reading threadsafe by adding proper locking around dictionary encoding operations. This likely addresses issue #420.

### Fix #582 (b8ebfc5) - Avoid race condition in multithreaded writes
Merged: Jan 12, 2026

This commit disabled multithreaded WRITING as a stopgap to fix a race condition in dictionary encoding. The commit message notes:

> "There is a race condition fundamental to the current architecture for creating and writing dictionary encodings. The relevant lock is created on a worker thread and thus there is a race to _create_ the lock and initialize the relevant data structure."

## Current Status

The issue likely has two components:
1. **Reading** (constructing Arrow.Table) - Fixed by #535
2. **Writing** (Arrow.write with partitions) - Mitigated by #582 (multithreaded writing disabled)

## Verification Needed

Test if the original reproduction still fails:

```julia
using Tables, Arrow, PooledArrays

for _ in 1:10000
    t = Tables.partitioner(
        (
            (a=Arrow.toarrowvector(PooledArray([1,2,3  ])),),
            (a=Arrow.toarrowvector(PooledArray([1,2,3,4])),),
            (a=Arrow.toarrowvector(PooledArray([1,2,3,4,5])),),
        )
    )
    tt = Arrow.Table(Arrow.tobuffer(t))
end
```

## Resolution

- [x] Reading side fixed by #535 (DictEncoding reading threadsafe)
- [x] Writing side mitigated by #582 (multithreaded writing disabled)
- [ ] Verify original reproduction no longer fails
- [ ] Close issue if fixed

## Recommendation

Run the reproduction test with the latest code. If it passes, close issue #420 referencing the fixes in #535 and #582.
