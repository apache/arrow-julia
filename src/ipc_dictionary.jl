# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Dictionary updates retain immutable snapshots. Concatenate physical storage,
# never materialized Julia values: Union routes, nested dictionary indices,
# logical descriptors, and null payloads must not pass through type inference.

mutable struct _DictionaryRange
    const data::ArrayData
    const first::Int64                 # zero-based logical position within data
    len::Int64
end

function _dictionaryvector(::Type{T}, n::Integer, limits::Limits, budget) where {T}
    bytes = AC.checked_mul(Int64(n), Int64(sizeof(T)))
    bytes <= limits.max_buffer_bytes ||
        throw(ValidationError("combined dictionary buffer exceeds max_buffer_bytes"))
    _chargevector!(budget, T, n, "dictionary concatenation")
    return zeros(T, n)
end

function _dictionaryrange!(ranges, data, first, len, budget)
    len == 0 && return ranges
    if !isempty(ranges)
        previous = last(ranges)
        if previous.data === data && previous.first + previous.len == first
            previous.len = AC.checked_add(previous.len, len)
            return ranges
        end
    end
    # Charge capacity growth before push!, including the range object.
    _chargeobject!(budget, 2 * sizeof(_DictionaryRange), "dictionary child ranges")
    push!(ranges, _DictionaryRange(data, first, len))
    return ranges
end

function _dictionaryput!(
    bytes::Vector{UInt8},
    offset::Integer,
    value::Integer,
    width::Int;
    signed=true,
)
    bits = 8 * width - signed
    maximum = bits >= 63 ? typemax(Int64) : (Int64(1) << bits) - 1
    0 <= value <= maximum ||
        throw(ValidationError("combined dictionary offset exceeds its integer width"))
    for k = 0:(width - 1)
        bytes[offset + k + 1] = UInt8((value >> (8 * k)) & 0xff)
    end
end

function _dictionarycopy!(out, dest, src::BufferSlice, first, len)
    len == 0 && return
    0 <= dest <= length(out) && 0 <= len <= length(out) - dest ||
        throw(ValidationError("dictionary copy escapes destination buffer"))
    # subslice checks the source before exposing bytes.
    slice = AC.subslice(src, first, len)
    GC.@preserve out slice begin
        unsafe_copyto!(pointer(out, dest + 1), AC.sliceptr(slice), len)
    end
end

function _dictionarylength(ranges, limits)
    n = Int64(0)
    for r in ranges
        n = AC.checked_add(n, r.len)
    end
    n <= limits.max_array_length ||
        throw(ValidationError("combined dictionary length exceeds max_array_length"))
    return n
end

# Also used recursively to compact selected child ranges. Every range points
# into a pool that has already passed full semantic validation.
function _dictionaryconcat(f::Field, ranges, limits::Limits, budget)
    n = _dictionarylength(ranges, limits)
    t = f.type
    spec = AC.layoutspec_of(t)
    _chargeobject!(budget, sizeof(ArrayData), "combined dictionary array")
    _chargevector!(budget, BufferSlice, length(spec.buffers), "dictionary buffers")
    _chargevector!(budget, ArrayData, length(f.children), "dictionary children")
    buffers = BufferSlice[]
    children = ArrayData[]
    dictionary = nothing
    nc = Int64(0)
    if !isempty(spec.buffers) && spec.buffers[1] == AC.VALIDITY
        validity = _dictionaryvector(UInt8, cld(n, 8), limits, budget)
        k = 0
        for r in ranges, i = (r.first + 1):(r.first + r.len)
            if AC.isvalid_at(r.data, i)
                validity[(k >> 3) + 1] |= UInt8(1 << (k & 7))
            else
                nc += 1
            end
            k += 1
        end
        push!(buffers, nc == 0 ? BufferSlice() : AC._databuffer(validity))
    end
    if t isa NullType
        nc = n
    elseif t isa Union{Utf8Type,BinaryType,ListType,ListViewType,MapType}
        width = spec.offsetwidth
        isview = t isa ListViewType
        isbinary = t isa Union{Utf8Type,BinaryType}
        offsets = _dictionaryvector(
            UInt8,
            AC.checked_mul(n + !isview, Int64(width)),
            limits,
            budget,
        )
        sizes =
            isview ?
            _dictionaryvector(UInt8, AC.checked_mul(n, Int64(width)), limits, budget) :
            UInt8[]
        childranges = _DictionaryRange[]
        total = Int64(0)
        k = Int64(0)
        # Compute lengths and child selection before allocating the payload.
        for r in ranges, i = (r.first + 1):(r.first + r.len)
            _dictionaryput!(offsets, k * width, total, width)
            if AC.isvalid_at(r.data, i)
                lo, hi =
                    isview ? AC._listview_range(t, r.data, i) :
                    AC._offsets_at(r.data, i, width == 8)
                len = isview ? hi : hi - lo
                isview && _dictionaryput!(sizes, k * width, len, width)
                total = AC.checked_add(total, len)
                if !isbinary
                    total <= limits.max_array_length || throw(
                        ValidationError(
                            "combined dictionary child exceeds max_array_length",
                        ),
                    )
                    _dictionaryrange!(childranges, r.data.children[1], lo, len, budget)
                end
            end
            k += 1
        end
        !isview && _dictionaryput!(offsets, n * width, total, width)
        push!(buffers, AC._databuffer(offsets))
        if isbinary
            payload = _dictionaryvector(UInt8, total, limits, budget)
            pos = Int64(0)
            for r in ranges, i = (r.first + 1):(r.first + r.len)
                AC.isvalid_at(r.data, i) || continue
                lo, hi = AC._offsets_at(r.data, i, width == 8)
                _dictionarycopy!(payload, pos, AC.rolebuffer(r.data, AC.DATA), lo, hi - lo)
                pos += hi - lo
            end
            push!(buffers, AC._databuffer(payload))
        else
            isview && push!(buffers, AC._databuffer(sizes))
            push!(children, _dictionaryconcat(f.children[1], childranges, limits, budget))
        end
    elseif t isa Union{StructType,FixedSizeListType} ||
           (t isa UnionType && t.mode == AC.SparseMode)
        if t isa UnionType
            ids = _dictionaryvector(UInt8, n, limits, budget)
            k = 0
            for r in ranges
                _dictionarycopy!(
                    ids,
                    k,
                    AC.rolebuffer(r.data, AC.TYPE_IDS),
                    r.data.offset + r.first,
                    r.len,
                )
                k += r.len
            end
            push!(buffers, AC._databuffer(ids))
        end
        width = t isa FixedSizeListType ? Int64(t.listsize) : Int64(1)
        for j in eachindex(f.children)
            childranges = _DictionaryRange[]
            for r in ranges
                _dictionaryrange!(
                    childranges,
                    r.data.children[j],
                    AC.checked_mul(r.data.offset + r.first, width),
                    AC.checked_mul(r.len, width),
                    budget,
                )
            end
            push!(children, _dictionaryconcat(f.children[j], childranges, limits, budget))
        end
    elseif t isa UnionType
        ids = _dictionaryvector(UInt8, n, limits, budget)
        offsets = _dictionaryvector(UInt8, AC.checked_mul(n, Int64(4)), limits, budget)
        _chargevector!(
            budget,
            Vector{_DictionaryRange},
            length(f.children),
            "dictionary union routes",
        )
        routes = [_DictionaryRange[] for _ in f.children]
        counts = _dictionaryvector(Int64, length(f.children), limits, budget)
        k = Int64(0)
        for r in ranges, i = (r.first + 1):(r.first + r.len)
            tid = AC.loadat(AC.rolebuffer(r.data, AC.TYPE_IDS), Int8, r.data.offset + i - 1)
            j = findfirst(==(tid), t.typeids)::Int
            off = Int64(
                AC.loadat(
                    AC.rolebuffer(r.data, AC.ELEMENT_OFFSETS),
                    Int32,
                    (r.data.offset + i - 1) * 4,
                ),
            )
            ids[k + 1] = reinterpret(UInt8, tid)
            _dictionaryput!(offsets, k * 4, counts[j], 4)
            _dictionaryrange!(routes[j], r.data.children[j], off, Int64(1), budget)
            counts[j] += 1
            k += 1
        end
        append!(buffers, (AC._databuffer(ids), AC._databuffer(offsets)))
        for j in eachindex(f.children)
            push!(children, _dictionaryconcat(f.children[j], routes[j], limits, budget))
        end
    elseif t isa RunEndEncodedType
        # Intersect physical runs with each requested logical range. This
        # handles sliced/overlong last runs without expanding one value per row.
        runranges = _DictionaryRange[]
        _chargevector!(budget, Int64, 0, "dictionary run ends")
        ends = Int64[]
        total = Int64(0)
        for r in ranges
            pos = r.first + 1
            stop = r.first + r.len
            while pos <= stop
                run = AC._ree_runindex(r.data, pos)
                runend =
                    Int64(AC.getvalue(f.children[1], r.data.children[1], run)) -
                    r.data.offset
                len = min(runend, stop) - pos + 1
                total = AC.checked_add(total, len)
                _chargeobject!(budget, 2 * sizeof(Int64), "dictionary run ends")
                push!(ends, total)
                _dictionaryrange!(runranges, r.data.children[2], run - 1, Int64(1), budget)
                pos += len
            end
        end
        _chargeobject!(budget, sizeof(ArrayData), "dictionary run-end array")
        _chargevector!(budget, BufferSlice, 2, "dictionary run-end buffers")
        rt = f.children[1].type::IntType
        width = AC.primwidth(rt)
        data = _dictionaryvector(
            UInt8,
            AC.checked_mul(Int64(length(ends)), Int64(width)),
            limits,
            budget,
        )
        for (i, value) in enumerate(ends)
            _dictionaryput!(data, (i - 1) * width, value, width)
        end
        push!(
            children,
            ArrayData(rt, length(ends), [BufferSlice(), AC._databuffer(data)]; nullcount=0),
        )
        push!(children, _dictionaryconcat(f.children[2], runranges, limits, budget))
    elseif t isa ViewType
        views = _dictionaryvector(UInt8, AC.checked_mul(n, Int64(16)), limits, budget)
        # Keep long-value buffers borrowed; only descriptors need rebasing.
        _chargedict!(budget, ArrayData, Int64, length(ranges), "dictionary view buffers")
        bases = IdDict{ArrayData,Int64}()
        databuffers = BufferSlice[]
        k = Int64(0)
        for r in ranges
            base = get(bases, r.data, Int64(-1))
            if base < 0
                base = Int64(length(databuffers))
                bases[r.data] = base
                _chargevector!(
                    budget,
                    BufferSlice,
                    2 * (length(r.data.buffers) - 2),
                    "dictionary view buffers",
                )
                append!(databuffers, r.data.buffers[3:end])
            end
            for i = (r.first + 1):(r.first + r.len)
                if AC.isvalid_at(r.data, i)
                    src = AC.rolebuffer(r.data, AC.VIEWS)
                    off = (r.data.offset + i - 1) * 16
                    _dictionarycopy!(views, k * 16, src, off, Int64(16))
                    if AC.loadat(src, Int32, off) > AC.VIEW_INLINE_MAX
                        idx = AC.loadat(src, Int32, off + 8)
                        _dictionaryput!(
                            views,
                            k * 16 + 8,
                            AC.checked_add(base, Int64(idx)),
                            4,
                        )
                    end
                end
                k += 1
            end
        end
        push!(buffers, AC._databuffer(views))
        append!(buffers, databuffers)
    elseif t isa DictionaryType
        # Nested dictionary values can refer to different snapshots. Concatenate
        # unique pools and rebase their indices, preserving their value routes.
        _chargedict!(budget, ArrayData, Int64, length(ranges), "nested dictionary pools")
        bases = IdDict{ArrayData,Int64}()
        poolranges = _DictionaryRange[]
        total = Int64(0)
        for r in ranges
            pool = r.data.dictionary::ArrayData
            if !haskey(bases, pool)
                bases[pool] = total
                _dictionaryrange!(poolranges, pool, Int64(0), pool.len, budget)
                total = AC.checked_add(total, pool.len)
            end
        end
        dictionary =
            length(poolranges) == 1 ? poolranges[1].data :
            _dictionaryconcat(AC.dictvaluefield(f, t), poolranges, limits, budget)
        width = AC.primwidth(t.indextype)
        data = _dictionaryvector(UInt8, AC.checked_mul(n, Int64(width)), limits, budget)
        k = Int64(0)
        for r in ranges, i = (r.first + 1):(r.first + r.len)
            if AC.isvalid_at(r.data, i)
                idx = AC._load_int(
                    AC.rolebuffer(r.data, AC.DATA),
                    t.indextype,
                    (r.data.offset + i - 1) * width,
                )
                value = AC.checked_add(bases[r.data.dictionary], Int64(idx))
                _dictionaryput!(data, k * width, value, width; signed=t.indextype.signed)
            end
            k += 1
        end
        push!(buffers, AC._databuffer(data))
    elseif t isa BoolType
        data = _dictionaryvector(UInt8, cld(n, 8), limits, budget)
        k = Int64(0)
        for r in ranges, i = (r.first + 1):(r.first + r.len)
            if AC.getbit(AC.rolebuffer(r.data, AC.DATA), r.data.offset + i - 1)
                data[(k >> 3) + 1] |= UInt8(1 << (k & 7))
            end
            k += 1
        end
        push!(buffers, AC._databuffer(data))
    else
        width = spec.fixedwidth
        width >= 0 || throw(ValidationError("unsupported dictionary value layout"))
        data = _dictionaryvector(UInt8, AC.checked_mul(n, Int64(width)), limits, budget)
        k = Int64(0)
        for r in ranges
            len = AC.checked_mul(r.len, Int64(width))
            _dictionarycopy!(
                data,
                k,
                AC.rolebuffer(r.data, AC.DATA),
                AC.checked_mul(r.data.offset + r.first, Int64(width)),
                len,
            )
            k += len
        end
        push!(buffers, AC._databuffer(data))
    end
    return ArrayData(t, n, buffers; children, dictionary, nullcount=nc)
end

function _dictionarytransition(id, isdelta, present; file=false)
    isdelta &&
        !present &&
        throw(ValidationError("delta dictionary id $id has no base dictionary"))
    file &&
        !isdelta &&
        present &&
        throw(ValidationError("dictionary replacement is forbidden in the IPC file format"))
    return nothing
end

function _updatedictionary!(
    dicts,
    validated,
    id,
    isdelta,
    vf,
    decoded,
    limits,
    budget;
    file=false,
)
    _dictionarytransition(id, isdelta, haskey(dicts, id); file)
    validate_semantic(vf, decoded)
    if isdelta
        previous = dicts[id]
        if decoded.len == 0
            return previous
        end
        _chargevector!(budget, _DictionaryRange, 2, "dictionary delta ranges")
        _chargeobject!(budget, 2 * sizeof(_DictionaryRange), "dictionary delta ranges")
        decoded = _dictionaryconcat(
            vf,
            [
                _DictionaryRange(previous, 0, previous.len),
                _DictionaryRange(decoded, 0, decoded.len),
            ],
            limits,
            budget,
        )
        validate_semantic(vf, decoded)
    end
    validated[decoded] = nothing
    dicts[id] = decoded
    return decoded
end
