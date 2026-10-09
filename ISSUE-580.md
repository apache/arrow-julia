# Issue #580 Triage Report

## Summary
Arrow.Stream does not work with non-seekable I/O (FIFOs, sockets, named pipes). Two separate issues in read and write paths.

## Issue Type
Bug

## Reproduction
- Reproduced: Yes (per issue reporter's detailed MWEs)
- Julia version tested: 1.x
- Package version tested: v2.8.0

## Root Cause Analysis

### Read-side Issue
**Location**: `src/table.jl:28`

```julia
tobytes(io::IOStream) = Mmap.mmap(io)
```

For non-seekable I/O like FIFOs:
- `filesize(io)` returns 0
- `Mmap.mmap(io)` returns an empty `UInt8[]`
- Result: Stream appears empty, no data read

**Fix**: Check if IO is seekable before attempting mmap, fallback to `Base.read(io)`:

```julia
function tobytes(io::IOStream)
    # Check if the stream is seekable (regular file vs FIFO/socket)
    try
        pos = position(io)
        seek(io, pos)  # Verify seeking works
        return Mmap.mmap(io)
    catch
        return Base.read(io)
    end
end
```

Or use a simpler heuristic based on `filesize`:

```julia
function tobytes(io::IOStream)
    # mmap doesn't work for non-seekable streams (FIFOs, etc.)
    # where filesize returns 0 but data is available
    fs = filesize(io)
    if fs > 0
        return Mmap.mmap(io)
    else
        return Base.read(io)
    end
end
```

### Write-side Issue
**Location**: `src/write.jl:569-573`

```julia
if msg.blockmsg
    push!(
        blocks[msg.isrecordbatch ? 1 : 2],
        Block(position(io), metalen + 8, msg.bodylen),
    )
end
```

The `blockmsg` flag is set in `makemessage()` based on message type (RecordBatch/DictionaryBatch), NOT based on whether we're writing file format. Block tracking calls `position(io)`, which fails on non-seekable streams.

**Location of blockmsg setting**: `src/write.jl:604-609`

```julia
Message(
    ...
    headerType == Meta.RecordBatch || headerType == Meta.DictionaryBatch,  # blockmsg - ALWAYS true for these types
    ...
)
```

**Fix Options**:

1. **Option A**: Pass `writetofile` flag to message creation and only set `blockmsg=true` when writing file format

2. **Option B**: Modify `Base.write(io::IO, msg::Message, ...)` to accept `writetofile` parameter and skip block tracking

3. **Option C**: Check if IO is seekable before calling `position(io)`:

```julia
if msg.blockmsg && applicable(position, io)
    try
        pos = position(io)
        push!(blocks[msg.isrecordbatch ? 1 : 2], Block(pos, metalen + 8, msg.bodylen))
    catch
        # Non-seekable stream, skip block tracking (only valid for streaming format)
    end
end
```

## Recommended Fix

The cleanest fix is **Option A** - explicitly track whether we need block positions:

1. Add `trackblocks::Bool` parameter to `makemessage()` or the `Message` struct
2. Set it to `writetofile` value when creating messages in Writer
3. Only record block positions when `trackblocks && msg.blockmsg`

## Resolution
- [ ] Fix `tobytes(io::IOStream)` to handle non-seekable I/O in `src/table.jl:28`
- [ ] Fix block tracking to not call `position(io)` for streaming format in `src/write.jl`
- [ ] Add tests with non-seekable I/O (can use `IOBuffer` or mock streams)

## Additional Notes
The issue reporter indicated they will submit a PR. This analysis can guide review of their PR.
