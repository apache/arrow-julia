# Issue #474 Triage Report

## Summary
Bus errors occur when writing DataFrames to arrow files.

## Issue Type
Bug (partial user error, but could be better handled)

## Root Cause Analysis

### Confirmed Case (stuartthomas25's reproduction)
When reading an arrow file with `Arrow.Table(filename)`, the file is memory-mapped via `Mmap.mmap()`. The returned `Arrow.Table` holds references to this mmap'd memory.

If you then call `Arrow.write(filename, table)` on the SAME file:
1. Opening for write truncates the file
2. The mmap'd memory becomes invalid
3. Reading from the table (to write) causes a bus error (SIGBUS)

**Reproduction:**
```julia
using Arrow

# Write initial data
Arrow.write("test.arrow", (a=[1,2,3],))

# Read it back (this mmaps the file)
t = Arrow.Table("test.arrow")

# Try to overwrite - CRASHES with bus error
Arrow.write("test.arrow", t)
```

### simsurace's Original Case (intermittent)
The original reporter mentions writing to DIFFERENT file names but still getting intermittent errors. This could be:
1. A different bug related to memory management
2. Threading issues (note the stack trace shows threading)
3. GC collecting mmap'd memory incorrectly

This case needs more investigation to reproduce.

## Potential Fixes

### Option 1: Documentation
Document clearly that you cannot write an Arrow.Table back to its source file without first copying the data.

### Option 2: Runtime Check
Before writing, check if any source arrays reference mmap'd memory pointing to the target file:

```julia
function write(file_path::String, tbl; kwargs...)
    # Check if tbl contains references to this file
    if has_mmap_reference_to_file(tbl, file_path)
        error("Cannot write to '$file_path': the source table contains data mmap'd from this file. Use Arrow.write(file_path, Arrow.Table(copy(tbl))) instead.")
    end
    # ... rest of write
end
```

### Option 3: Automatic Copy
Add a `copy=false` keyword argument. When `true`, copy all data before writing:

```julia
Arrow.write("test.arrow", t; copy=true)  # Safe overwrite
```

## Recommendation

1. **Short-term**: Add documentation warning about this behavior
2. **Medium-term**: Implement Option 2 (runtime check with helpful error message)
3. **Long-term**: Consider Option 3 for convenience

## Resolution Status
- [ ] Document mmap behavior and write-to-same-file limitation
- [ ] Consider adding runtime detection/warning
- [ ] Investigate simsurace's intermittent case separately (may be different bug)

## Additional Notes
The workaround mentioned by @sprig (write to temp file, then rename) is reasonable but has atomicity concerns. The safest workaround is:

```julia
t = Arrow.Table("data.arrow")
# Copy all columns to regular Julia arrays
copied = (;(k => copy(v) for (k, v) in pairs(Tables.columntable(t)))...)
Arrow.write("data.arrow", copied)
```
