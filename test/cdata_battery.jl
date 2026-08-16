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

function cdata_battery()
    if Sys.WORD_SIZE == 64
        @assert sizeof(CArrowSchema) == 72
        @assert fieldoffset.(Ref(CArrowSchema), 1:9) == 0:8:64
        @assert sizeof(CArrowArray) == 80
        @assert fieldoffset.(Ref(CArrowArray), 1:10) == 0:8:72
    elseif Sys.WORD_SIZE == 32
        if Base.datatype_alignment(Int64) == 4 # i686 SysV ABI
            @assert sizeof(CArrowSchema) == 44
            @assert fieldoffset.(Ref(CArrowSchema), 1:9) == [0, 4, 8, 12, 20, 28, 32, 36, 40]
            @assert sizeof(CArrowArray) == 60
            @assert fieldoffset.(Ref(CArrowArray), 1:10) == [0, 8, 16, 24, 32, 40, 44, 48, 52, 56]
        else # 32-bit ABIs that align int64_t to 8 bytes
            @assert sizeof(CArrowSchema) == 48
            @assert fieldoffset.(Ref(CArrowSchema), 1:9) == [0, 4, 8, 16, 24, 32, 36, 40, 44]
            @assert sizeof(CArrowArray) == 64
            @assert fieldoffset.(Ref(CArrowArray), 1:10) == [0, 8, 16, 24, 32, 40, 44, 48, 52, 56]
        end
    else
        error("unsupported pointer width $(Sys.WORD_SIZE)")
    end
    println("C ABI size and field-offset gate passed for $(Sys.WORD_SIZE)-bit ✓")

    # A reaper may run while an export tree is being built. Partial mallocs
    # must stay private until the finished tree is published.
    before = _registry_count()
    entered = Base.Event()
    finish = Base.Event()
    builder = @async _newroot(Any[]) do root
        p = _malloc!(root, 64)
        notify(entered)
        wait(finish)
        @assert !isempty(root.mallocs)
        p
    end
    wait(entered)
    @assert _registry_count() == before
    @assert reap!() == 0
    notify(finish)
    fetch(builder)
    @assert _registry_count() == before + 1
    @assert reap!() == 1
    @assert _registry_count() == before
    println("in-progress exports are hidden from the reaper ✓")

    # Every native allocation and source lifetime must have an owner before the
    # next fallible operation. Inject failures at each ownership handoff.
    deallocations = Ref(0)
    @assert try
        _newroot(Any[]) do root
            _malloc!(root, 64,
                (_ledger, _p) -> error("injected malloc registration failure"),
                p -> begin
                    deallocations[] += 1
                    Libc.free(p)
                end)
        end
        false
    catch e
        e isa ErrorException &&
            e.msg == "injected malloc registration failure"
    end
    @assert deallocations[] == 1
    @assert _registry_count() == before
    # The allocator result is owned before the first later fallible action.
    # A registration method may append successfully and fail before it
    # returns. In that state root cleanup, not the local catch, owns the entry.
    innerdeallocations = Ref(0)
    @assert try
        _newroot(Any[]) do root
            _malloc!(root, 64,
                (ledger, p) -> begin
                    push!(ledger, p)
                    error("injected post-registration failure")
                end,
                _ -> (innerdeallocations[] += 1))
        end
        false
    catch e
        e isa ErrorException &&
            e.msg == "injected post-registration failure"
    end
    @assert innerdeallocations[] == 0
    @assert _registry_count() == before

    # Published schema and array roots do not transfer until the result tuple
    # reaches the caller. Failure at either return boundary cleans both roots.
    handofff, handoffd = fromjulia("export-handoff", Int64[1])
    handoff_arel = @cfunction(_release_array, Cvoid, (Ptr{CArrowArray},))
    handoff_srel = @cfunction(_release_schema, Cvoid, (Ptr{CArrowSchema},))
    # Plain build + cleanup releases both roots and empties the slots.
    sp_slot = Ref{Ptr{CArrowSchema}}(C_NULL)
    skey_slot = Ref{Int64}(0)
    ap_slot = Ref{Ptr{CArrowArray}}(C_NULL)
    akey_slot = Ref{Int64}(0)
    _build_c_data!(sp_slot, skey_slot, ap_slot, akey_slot,
        handofff, handoffd, handoff_arel, handoff_srel)
    _cleanup_export_slots!(sp_slot, skey_slot, ap_slot, akey_slot)
    @assert sp_slot[] == C_NULL && ap_slot[] == C_NULL
    @assert skey_slot[] == 0 && akey_slot[] == 0
    @assert _registry_count() == before
    println("failed export handoffs return every malloc and registry root ✓")

    # Reap claims a fully released root by removing it from the registry
    # first, then freeing. Frees cannot fail, so no retry protocol exists —
    # the claim IS the removal.
    _, cleanup_data = fromjulia("cleanup", Int64[1])
    cleanup_key = Ref{Int64}(0)
    _newroot(Any[cleanup_data]) do root
        cleanup_key[] = root.key
        _malloc!(root, 64)
        _malloc!(root, 64)
        return nothing
    end
    @assert lock(REGISTRY_LOCK) do
        length(EXPORT_REGISTRY[cleanup_key[]].mallocs) == 2
    end
    @assert reap!() == 1
    @assert lock(REGISTRY_LOCK) do
        !haskey(EXPORT_REGISTRY, cleanup_key[])
    end
    println("reap claims by registry removal and frees every malloc ✓")

    # The registry, not the caller's Julia variables, must keep all source
    # objects and their buffers alive while raw C pointers are outstanding.
    sp, ap, dataref, regionref = _export_and_forget()
    GC.gc(true)
    @assert dataref.value !== nothing
    @assert regionref.value !== nothing
    rootedf, rootedd = from_c_data(sp, ap)
    @assert materialize(rootedf, rootedd) == [1, 2]
    @assert reap!() == 1
    release!(rootedd.owner::ForeignOwner)
    @assert reap!() == 1
    @assert _registry_count() == before
    println("export registry roots dropped Julia sources across GC ✓")

    b = batch((
        xs=Int64[1, 2, 3, 4],
        ys=[1.5, missing, 3.5, missing],
        strs=["a", "", missing, "δεζ"],
        lists=[[1, 2], missing, Int64[], [3]],
    ))
    expected = Dict(
        "xs" => Any[1, 2, 3, 4],
        "ys" => Any[1.5, missing, 3.5, missing],
        "strs" => Any["a", "", missing, "δεζ"],
        "lists" => Any[[1, 2], missing, Int64[], [3]],
    )

    imported = Tuple{Field,ArrayData}[]
    for (f, col) in zip(b.schema.fields, b.columns)
        sp, ap = to_c_data(f, col)
        f2, d2 = from_c_data(sp, ap)
        push!(imported, (f2, d2))
    end
    for (f2, d2) in imported
        got = materialize(f2, d2)
        @assert isequal(collect(Any, got), expected[f2.name]) "$(f2.name): $got"
    end
    println("export → import round-trip for $(length(imported)) columns ✓")
    nlive = _registry_count()
    @assert nlive == 2 * length(imported)
    println("live exports rooted in registry: $nlive")

    # Consumer-side release: drop the imported columns (their ForeignOwners'
    # release calls the exported arrays' release callbacks), then reap.
    for (_, d2) in imported
        release!(d2.owner::ForeignOwner)
    end
    reaped = reap!()
    println("reaped $reaped released exports ✓")

    # Double-release is inert: release the same owners again.
    for (_, d2) in imported
        release!(d2.owner::ForeignOwner)
    end
    @assert reap!() == 0
    println("double release is exactly-once ✓")

    # Explicit owner release is one call for the whole imported tree — no
    # per-buffer close exists. What it does NOT do is revoke access: touching
    # a slice after an explicit release! is undefined behavior, exactly the
    # post-release rule the C Data spec imposes on its own consumers. The
    # checkable contract is the exactly-once flag every owner carries.
    for (_, d2) in imported
        @assert (@atomic (d2.owner::ForeignOwner).released)
    end
    println("released owners are flagged; post-release access is out of contract ✓")

    # Format parity with Core's accessor set: every mapped descriptor
    # round-trips its format string, declared geometry, and values through
    # the raw C ABI. Ground truth is the SOURCE column's materialization.
    fslu, _ = fromjulia("fsl-child", Int64[1, 2, 3, 4])
    sui, sud = fromjulia("i", Int64[10, 20, 30])
    sus, susd = fromjulia("s", ["x", "y", "z"])
    dui, duid = fromjulia("i", Int64[10, 30])
    dus, dusd = fromjulia("s", ["y"])
    sut = UnionType(AC.SparseMode, Int8[0, 1])
    dut = UnionType(AC.DenseMode, Int8[0, 1])
    tsnulls = TimestampType(AC.MICROSECOND, "UTC")
    nestedirf, nestedird = fromjulia("run_ends", Int32[1, 2])
    nestedivf, nestedivd = fromjulia("values", Int64[10, 20])
    nestedinnerf = Field("values", RunEndEncodedType();
        children=[nestedirf, nestedivf])
    nestedinnerd = ArrayData(RunEndEncodedType(), 2, BufferSlice[];
        children=[nestedird, nestedivd], nullcount=0)
    nestedorf, nestedord = fromjulia("run_ends", Int32[2, 4])
    paritycases = Tuple{Field,ArrayData}[
        (Field("dec128", DecimalType(38, 10, 128)),
            ArrayData(DecimalType(38, 10, 128), 2,
                [BufferSlice(), AC._databuffer(Int128[123, -456])]; nullcount=0)),
        (Field("dec32", DecimalType(9, 2, 32)),
            ArrayData(DecimalType(9, 2, 32), 2,
                [BufferSlice(), AC._databuffer(Int32[1234, -5678])]; nullcount=0)),
        (Field("date32", DateType(AC.DAY)),
            ArrayData(DateType(AC.DAY), 2,
                [BufferSlice(), AC._databuffer(Int32[0, 19000])]; nullcount=0)),
        (Field("date64", DateType(AC.MILLISECOND_DATE)),
            ArrayData(DateType(AC.MILLISECOND_DATE), 2,
                [BufferSlice(), AC._databuffer(Int64[0, 86_400_000])]; nullcount=0)),
        (Field("time32s", TimeType(AC.SECOND, 32)),
            ArrayData(TimeType(AC.SECOND, 32), 2,
                [BufferSlice(), AC._databuffer(Int32[0, 86_399])]; nullcount=0)),
        (Field("time64n", TimeType(AC.NANOSECOND, 64)),
            ArrayData(TimeType(AC.NANOSECOND, 64), 2,
                [BufferSlice(), AC._databuffer(Int64[0, 12_345])]; nullcount=0)),
        (Field("ts-utc", tsnulls),
            ArrayData(tsnulls, 3,
                [AC._databuffer(UInt8[0x05]), AC._databuffer(Int64[7, 0, 9])];
                nullcount=1)),
        (Field("ts-naive", TimestampType(AC.SECOND, nothing)),
            ArrayData(TimestampType(AC.SECOND, nothing), 1,
                [BufferSlice(), AC._databuffer(Int64[42])]; nullcount=0)),
        (Field("dur", DurationType(AC.MILLISECOND)),
            ArrayData(DurationType(AC.MILLISECOND), 2,
                [BufferSlice(), AC._databuffer(Int64[5, -5])]; nullcount=0)),
        (Field("iym", IntervalType(AC.YEAR_MONTH)),
            ArrayData(IntervalType(AC.YEAR_MONTH), 2,
                [BufferSlice(), AC._databuffer(Int32[12, -1])]; nullcount=0)),
        (Field("idt", IntervalType(AC.DAY_TIME)),
            ArrayData(IntervalType(AC.DAY_TIME), 2,
                [BufferSlice(), AC._databuffer(Int32[1, 2, 3, 4])]; nullcount=0)),
        (Field("imdn", IntervalType(AC.MONTH_DAY_NANO)),
            ArrayData(IntervalType(AC.MONTH_DAY_NANO), 1,
                [BufferSlice(), AC._databuffer(
                    vcat(reinterpret(UInt8, Int32[1, 2]),
                        reinterpret(UInt8, Int64[3])))]; nullcount=0)),
        (Field("fsb", FixedSizeBinaryType(3)),
            ArrayData(FixedSizeBinaryType(3), 2,
                [BufferSlice(), AC._databuffer(collect(codeunits("abcdef")))]; nullcount=0)),
        (Field("fsl", FixedSizeListType(2); children=[fslu]),
            ArrayData(FixedSizeListType(2), 2, [BufferSlice()];
                children=[fromjulia("fsl-child", Int64[1, 2, 3, 4])[2]],
                nullcount=0)),
        (Field("lu", Utf8Type(true)),
            ArrayData(Utf8Type(true), 2,
                [BufferSlice(), AC._databuffer(Int64[0, 1, 3]),
                 AC._databuffer(collect(codeunits("abc")))]; nullcount=0)),
        (Field("lz", BinaryType(true)),
            ArrayData(BinaryType(true), 2,
                [BufferSlice(), AC._databuffer(Int64[0, 2, 3]),
                 AC._databuffer(UInt8[0x01, 0x02, 0x03])]; nullcount=0)),
        (Field("ll", ListType(true); children=[fslu]),
            ArrayData(ListType(true), 2,
                [BufferSlice(), AC._databuffer(Int64[0, 2, 4])];
                children=[fromjulia("fsl-child", Int64[1, 2, 3, 4])[2]],
                nullcount=0)),
        (Field("su", sut; nullable=false, children=[sui, sus]),
            ArrayData(sut, 3, [AC._databuffer(Int8[0, 1, 0])];
                children=[sud, susd], nullcount=0)),
        (Field("du", dut; nullable=false, children=[dui, dus]),
            ArrayData(dut, 3,
                [AC._databuffer(Int8[0, 1, 0]), AC._databuffer(Int32[0, 0, 1])];
                children=[duid, dusd], nullcount=0)),
        (Field("nulls", NullType()),
            ArrayData(NullType(), 3, BufferSlice[]; nullcount=3)),
        # format 1.3/1.4: views (with the C-only trailing sizes buffer),
        # list-views (per-slot offsets+sizes, unordered/overlapping), REE
        (Field("vu", ViewType(true); nullable=true),
            ArrayData(ViewType(true), 3,
                [AC._databuffer(UInt8[0x05]),
                 AC._databuffer(vcat(
                    _viewentry(3, collect(codeunits("abc"))),
                    _viewlong(25, collect(codeunits("firs")), 0, 0),
                    _viewlong(26, collect(codeunits("seco")), 1, 0))),
                 AC._databuffer(collect(codeunits("first-out-of-line-payload"))),
                 AC._databuffer(collect(codeunits("second-buffer-payload-here")))];
                nullcount=1)),
        (Field("vz", ViewType(false)),
            ArrayData(ViewType(false), 1,
                [BufferSlice(), AC._databuffer(_viewentry(2, UInt8[0xff, 0x00]))];
                nullcount=0)),
        (Field("lv", ListViewType(false); children=[fslu]),
            ArrayData(ListViewType(false), 3,
                [BufferSlice(), AC._databuffer(Int32[2, 0, 0]),
                 AC._databuffer(Int32[2, 2, 4])];
                children=[fromjulia("fsl-child", Int64[1, 2, 3, 4])[2]],
                nullcount=0)),
        (Field("Lv", ListViewType(true); children=[fslu]),
            ArrayData(ListViewType(true), 1,
                [BufferSlice(), AC._databuffer(Int64[1]), AC._databuffer(Int64[3])];
                children=[fromjulia("fsl-child", Int64[1, 2, 3, 4])[2]],
                nullcount=0)),
        (Field("ree", RunEndEncodedType(); children=[
                Field("run_ends", IntType(32, true); nullable=false),
                Field("values", Utf8Type(false); nullable=true)]),
            ArrayData(RunEndEncodedType(), 4, BufferSlice[];
                children=[fromjulia("run_ends", Int32[2, 3, 4])[2],
                          fromjulia("values", Union{Missing,String}["x", missing, "z"])[2]],
                nullcount=0)),
        (Field("nested-ree", RunEndEncodedType();
                children=[nestedorf, nestedinnerf]),
            ArrayData(RunEndEncodedType(), 4, BufferSlice[];
                children=[nestedord, nestedinnerd], nullcount=0)),
    ]
    for (f, d) in paritycases
        want = collect(Any, materialize(f, d))
        sp, ap = to_c_data(f, d)
        f2, d2 = from_c_data(sp, ap)
        @assert AC.typeequal(f2.type, f.type) f.name
        @assert isequal(collect(Any, materialize(f2, d2)), want) f.name
        release!(d2.owner::ForeignOwner)
    end
    @assert reap!() == 2 * length(paritycases)
    println("format parity round-trips for $(length(paritycases)) descriptor shapes ✓")

    # Format-string spot checks and refusals.
    @assert formatstring(DecimalType(38, 10, 128)) == "d:38,10"
    @assert formatstring(DecimalType(9, 2, 32)) == "d:9,2,32"
    @assert formatstring(TimestampType(AC.MICROSECOND, "UTC")) == "tsu:UTC"
    @assert formatstring(TimestampType(AC.SECOND, nothing)) == "tss:"
    @assert formatstring(IntervalType(AC.MONTH_DAY_NANO)) == "tin"
    @assert formatstring(UnionType(AC.DenseMode, Int8[0, 1])) == "+ud:0,1"
    @assert formatstring(FixedSizeListType(2)) == "+w:2"
    @assert parseformat("tsu:UTC") == TimestampType(AC.MICROSECOND, "UTC")
    @assert parseformat("tsu:Δ") == TimestampType(AC.MICROSECOND, "Δ")
    @assert parseformat("d:38,10") == DecimalType(38, 10, 128)
    @assert parseformat("d:38,-2") == DecimalType(38, -2, 128)
    @assert parseformat("vu") == ViewType(true) && formatstring(ViewType(true)) == "vu"
    @assert parseformat("vz") == ViewType(false) && formatstring(ViewType(false)) == "vz"
    @assert parseformat("+vl") == ListViewType(false)
    @assert parseformat("+vL") == ListViewType(true) &&
        formatstring(ListViewType(true)) == "+vL"
    @assert parseformat("+r") == RunEndEncodedType() &&
        formatstring(RunEndEncodedType()) == "+r"
    badformats = String[
        "v", "vx", "+v", "+vx", "+rr", "d:x", "w:", "tsq:",
        "tsé:", "ts💣:", "tsu:UTC\0hidden",
        "w: 1", "w:1 ", "w:+1", "w:0x10", "+w: 2",
        "d: 1,0", "d:1, 0", "d:+1,+0", "d:0x9,0x2,0x20",
        "d:0,0", "d:39,0", "d:1,0,1", "d:77,0,256",
        "+ud:200", "+ud:0,0", "+ud: 0,1", "+us:+1",
        "+ud:0x0,0x1", "+ud:" * join(0:128, ","),
    ]
    push!(badformats, String(UInt8[0x74, 0x73, 0x75, 0x3a, 0xff]))
    for bad in badformats
        @assert try
            parseformat(bad)
            false
        catch e
            e isa ValidationError
        end (bad)
    end
    println("format strings use strict byte-safe grammar and reject corrupt forms ✓")

    # Core can omit the physical offsets allocation for a canonical empty
    # array. C Data still requires its length+1 terminal offset. The export
    # aggregate owns that adapter-only zero until the consumer releases it.
    emptyitemf, emptyitemd = fromjulia("item", Int64[])
    emptyoffsetcases = Tuple{Field,ArrayData}[]
    for t in (Utf8Type(false), Utf8Type(true), BinaryType(false), BinaryType(true))
        push!(emptyoffsetcases, (Field("empty", t),
            ArrayData(t, 0, [BufferSlice(), BufferSlice(), BufferSlice()];
                nullcount=0)))
    end
    for t in (ListType(false), ListType(true))
        push!(emptyoffsetcases, (Field("empty-list", t; children=[emptyitemf]),
            ArrayData(t, 0, [BufferSlice(), BufferSlice()];
                children=[emptyitemd], nullcount=0)))
    end
    emptykeyt = Utf8Type(false)
    emptykeyf = Field("key", emptykeyt; nullable=false)
    emptykeyd = ArrayData(emptykeyt, 0,
        [BufferSlice(), BufferSlice(), BufferSlice()]; nullcount=0)
    emptyvaluef, emptyvalued = fromjulia("value", Int64[])
    emptyentriesf = Field("entries", StructType(); nullable=false,
        children=[emptykeyf, emptyvaluef])
    emptyentriesd = ArrayData(StructType(), 0, [BufferSlice()];
        children=[emptykeyd, emptyvalued], nullcount=0)
    emptymapt = MapType(false)
    push!(emptyoffsetcases, (Field("empty-map", emptymapt;
        children=[emptyentriesf]),
        ArrayData(emptymapt, 0, [BufferSlice(), BufferSlice()];
            children=[emptyentriesd], nullcount=0)))
    for (f, d) in emptyoffsetcases
        spec = layoutspec(f.type)
        oi = findfirst(==(AC.OFFSETS), spec.buffers)::Int
        sp, ap = to_c_data(f, d)
        arr = unsafe_load(ap)
        offsetp = Ptr{UInt8}(unsafe_load(arr.buffers, oi))
        @assert offsetp != C_NULL
        GC.gc(true)
        @assert spec.offsetwidth == 4 ?
            unsafe_load(Ptr{Int32}(offsetp)) == 0 :
            unsafe_load(Ptr{Int64}(offsetp)) == 0
        f2, d2 = from_c_data(sp, ap)
        @assert d2.buffers[oi].len == spec.offsetwidth
        @assert isempty(materialize(f2, d2))
        release!(d2.owner::ForeignOwner)
        @assert reap!() == 2
    end
    println("empty C Data offset layouts export one rooted terminal zero ✓")

    nullf = Field("null-empty", Utf8Type(false))
    nulld = ArrayData(Utf8Type(false), 0,
        [BufferSlice(), BufferSlice(), BufferSlice()]; nullcount=0)
    sp, ap = to_c_data(nullf, nulld)
    unsafe_store!(unsafe_load(ap).buffers, Ptr{Cvoid}(C_NULL), 2)
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError && occursin("NULL OFFSETS buffer", e.msg)
    end
    @assert reap!() == 2
    println("NULL empty C Data offsets fail with exact cleanup ✓")

    # Descriptor and union shape failures must happen before malformed
    # metadata can direct recursive or fixed-width geometry work.
    earlyf, earlyd = fromjulia("early", Int64[1])
    sp, ap = to_c_data(earlyf, earlyd)
    baddecimal = "d:1,0,2147483647"
    GC.@preserve baddecimal begin
        _store_field!(sp, :format, pointer(baddecimal))
        _store_field!(ap, :length, typemax(Int64))
        @assert try
            from_c_data(sp, ap)
            false
        catch e
            e isa ValidationError
        end
    end
    @assert reap!() == 2

    earlyunionf = Field("early-union", sut; children=[sui, sus])
    earlyuniond = ArrayData(sut, 3, [AC._databuffer(Int8[0, 1, 0])];
        children=[sud, susd], nullcount=0)
    sp, ap = to_c_data(earlyunionf, earlyuniond)
    shortunion = "+us:0"
    badchild = "not-a-format"
    firstchild = unsafe_load(unsafe_load(sp).children, 1)
    GC.@preserve shortunion badchild begin
        _store_field!(sp, :format, pointer(shortunion))
        _store_field!(firstchild, :format, pointer(badchild))
        @assert try
            from_c_data(sp, ap)
            false
        catch e
            e isa ValidationError && occursin("type ids", e.msg)
        end
    end
    @assert reap!() == 2
    println("invalid descriptors and union counts fail before geometry/children ✓")

    # A negative final variable-length offset cannot become a negative foreign
    # region extent. Reject it at the adapter boundary with ValidationError.
    negativef, negatived = fromjulia("negative-offset", ["x"])
    sp, ap = to_c_data(negativef, negatived)
    offsetp = Ptr{Int32}(unsafe_load(unsafe_load(ap).buffers, 2))
    unsafe_store!(offsetp, Int32(-1), 2)
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError && occursin("negative final offset", e.msg)
    end
    @assert reap!() == 2
    println("negative C Data final offsets fail cleanly ✓")

    # Import of an already-released structure is refused.
    f, col = b.schema.fields[1], b.columns[1]
    sp, ap = to_c_data(f, col)
    _f, _d = from_c_data(sp, ap)      # moves: source release now NULL
    caught = try
        from_c_data(sp, ap)
        false
    catch e
        e isa ArgumentError
    end
    @assert caught
    release!(_d.owner::ForeignOwner)
    @assert reap!() == 2
    println("moved (released) source cannot be imported twice ✓")



    # Schema cleanup is installed before owner construction. If construction
    # fails, the array remains with its source while the schema is released.
    cf, cd = fromjulia("owner-construction", Int64[1])
    cbefore = _registry_count()
    sp, ap = to_c_data(cf, cd)
    @assert try
        _from_c_data(sp, ap;
            ownerfactory=_ -> error("injected owner construction failure"))
        false
    catch e
        e isa ErrorException &&
            e.msg == "injected owner construction failure"
    end
    @assert unsafe_load(sp).release == C_NULL
    @assert unsafe_load(ap).release != C_NULL
    @assert reap!() == 1                       # schema root only
    @assert _registry_count() == cbefore + 1   # array root still owed to source
    _call_release(ap)
    @assert reap!() == 1
    @assert _registry_count() == cbefore

    # Finalizer registration is the last ownership handoff in construction.
    # If a registrar installs the finalizer and then throws, constructor
    # cleanup frees the inert malloc'd copy without releasing the producer.
    rf, rd = fromjulia("finalizer-registration", Int64[1])
    rbefore = _registry_count()
    sp, ap = to_c_data(rf, rd)
    _release_c_schema!(sp, unsafe_load(sp))
    captured_owner = Ref{Any}(nothing)
    failing_registrar = (f, o) -> begin
        captured_owner[] = o
        finalizer(f, o)
        error("injected post-registration failure")
    end
    @assert try
        ForeignOwner(unsafe_load(ap), failing_registrar)
        false
    catch e
        e isa ErrorException &&
            e.msg == "injected post-registration failure"
    end
    failed_owner = captured_owner[]::ForeignOwner
    @assert (@atomic failed_owner.released)
    @assert unsafe_load(ap).release != C_NULL
    finalize(failed_owner)
    release!(failed_owner)
    @assert unsafe_load(ap).release != C_NULL
    @assert reap!() == 1                       # schema root only
    _call_release(ap)
    @assert reap!() == 1
    @assert _registry_count() == rbefore
    println("failed finalizer registration frees only the inert owner copy ✓")

    # A producer that violates release=NULL still loses its stable copy once,
    # reports the conformance error, and leaves every later release inert.
    before_calls = TEST_NONCONFORMING_RELEASES[]
    deallocations = Ref(0)
    nonconforming_owner =
        ForeignOwner(_test_c_array(test_nonconforming_release()))
    _arm_foreign_owner!(nonconforming_owner)
    @assert try
        _release_foreign_owner!(nonconforming_owner, p -> begin
            deallocations[] += 1
            Libc.free(p)
        end)
        false
    catch e
        e isa ErrorException &&
            e.msg == "C Data producer release did not mark the structure released"
    end
    @assert deallocations[] == 1
    @assert TEST_NONCONFORMING_RELEASES[] == before_calls + 1
    finalize(nonconforming_owner)
    release!(nonconforming_owner)
    @assert TEST_NONCONFORMING_RELEASES[] == before_calls + 1
    # Explicit `finalize` exercises the registered finalizer's error path.
    # Julia reports finalizer errors instead of throwing them to this caller,
    # so suppress the expected diagnostic and verify the durable state.
    finalizer_error_owner =
        ForeignOwner(_test_c_array(test_nonconforming_release()))
    _arm_foreign_owner!(finalizer_error_owner)
    redirect_stderr(devnull) do
        finalize(finalizer_error_owner)
    end
    @assert (@atomic finalizer_error_owner.released)
    @assert TEST_NONCONFORMING_RELEASES[] == before_calls + 2
    release!(finalizer_error_owner)
    println("nonconforming producer release frees once and reports the error ✓")

    # Producer C callbacks have no error channel. release! calls the
    # persistent malloc'd copy once, checks the producer nulled the copy's
    # release field (the C Data conformance rule), then frees the copy.
    pf, pd = fromjulia("producer-release", Int64[1])
    sp, ap = to_c_data(pf, pd)
    _release_c_schema!(sp, unsafe_load(sp))
    arr = unsafe_load(ap)
    producer_owner = ForeignOwner(arr)
    @assert !_foreign_owner_armed(producer_owner)  # inert until the move commits
    _store_field!(ap, :release, Ptr{Cvoid}(C_NULL))
    _arm_foreign_owner!(producer_owner)
    @assert _foreign_owner_armed(producer_owner)
    release!(producer_owner)
    @assert (@atomic producer_owner.released)
    release!(producer_owner)                       # idempotent
    @assert reap!() == 2
    println("producer release is one committed, conformance-checked step ✓")

    # A root release must transitively release every child. Inspect before
    # reap, while the exported structs remain allocated.
    lf, ld = b.schema.fields[4], b.columns[4]
    sp, ap = to_c_data(lf, ld)
    schild = unsafe_load(unsafe_load(sp).children, 1)
    achild = unsafe_load(unsafe_load(ap).children, 1)
    _call_release(sp)
    _call_release(ap)
    @assert unsafe_load(sp).release == C_NULL
    @assert unsafe_load(schild).release == C_NULL
    @assert unsafe_load(ap).release == C_NULL
    @assert unsafe_load(achild).release == C_NULL
    @assert reap!() == 2
    println("root release is transitive across child trees ✓")

    # C Data move semantics permit a consumer to shallow-copy a child and
    # null the source child's release field. The parent must skip that child,
    # and the aggregate allocation must remain live until the moved copy is
    # released independently.
    sp, ap = to_c_data(lf, ld)
    schild = unsafe_load(unsafe_load(sp).children, 1)
    achild = unsafe_load(unsafe_load(ap).children, 1)
    smoved = Ref(unsafe_load(schild))
    amoved = Ref(unsafe_load(achild))
    _store_field!(schild, :release, Ptr{Cvoid}(C_NULL))
    _store_field!(achild, :release, Ptr{Cvoid}(C_NULL))
    _call_release(sp)
    _call_release(ap)
    @assert reap!() == 0
    @assert _registry_count() == 2
    GC.@preserve smoved amoved begin
        smovedp = Base.unsafe_convert(Ptr{CArrowSchema}, smoved)
        amovedp = Base.unsafe_convert(Ptr{CArrowArray}, amoved)
        @assert unsafe_load(smovedp).release != C_NULL
        @assert unsafe_load(amovedp).release != C_NULL
        movedf, movedd = from_c_data(smovedp, amovedp)
        @assert materialize(movedf, movedd) == [1, 2, 3]
        release!(movedd.owner::ForeignOwner)
    end
    @assert reap!() == 2
    println("moved children retain aggregate ownership until release ✓")

    # The void C release entrypoints are claim/commit transactions with no
    # error channel: a completed release commits exactly once, and a repeat
    # call on a released structure is inert.
    rf, rd = fromjulia("plain-release", Int64[1])
    sp, ap = to_c_data(rf, rd)
    acontrol = unsafe_load(ap).private_data
    _call_release(ap)
    @assert unsafe_load(Ptr{UInt8}(acontrol)) == 0x02
    @assert unsafe_load(ap).release == C_NULL
    _call_release(ap)   # inert repeat
    @assert reap!() == 1
    _call_release(sp)
    @assert reap!() == 1
    println("C release entrypoints commit exactly once and repeats are inert ✓")

    # A persistent internal error must not spin forever inside the void C
    # callback. The claimed parent returns to LIVE. Completed descendants
    # stay NULL, and a later explicit call can resume safely.
    retryf, retryd = fromjulia("child", Int64[1])
    retrysf = Field("parent", StructType(); children=[retryf])
    retrysd = ArrayData(StructType(), 1, [BufferSlice()];
        children=[retryd], nullcount=0)
    sp, ap = to_c_data(retrysf, retrysd)
    parentcontrol = unsafe_load(ap).private_data
    childp = unsafe_load(unsafe_load(ap).children, 1)
    childcontrol = unsafe_load(childp).private_data
    retrykey = unsafe_load(Ptr{Int64}(parentcontrol + 8))
    childtopology = lock(REGISTRY_LOCK) do
        pop!(EXPORT_REGISTRY[retrykey].array_topology, childcontrol)
    end
    _call_release(ap)
    @assert unsafe_load(ap).release != C_NULL
    @assert unsafe_load(childp).release != C_NULL
    @assert unsafe_load(Ptr{UInt8}(parentcontrol)) == 0x00
    lock(REGISTRY_LOCK) do
        EXPORT_REGISTRY[retrykey].array_topology[childcontrol] = childtopology
    end
    _call_release(ap)
    @assert unsafe_load(ap).release == C_NULL
    @assert unsafe_load(childp).release == C_NULL
    _call_release(sp)
    @assert reap!() == 2
    println("failed C release callbacks return LIVE and resume on a later call ✓")

    # Schema/data mismatch and malformed buffers must fail before either
    # independently-owned export root is published.
    before = _registry_count()
    mf = Field("wrong", IntType(32, true); nullable=false)
    _, md = fromjulia("wrong", Int64[1])
    @assert try
        to_c_data(mf, md)
        false
    catch e
        e isa ValidationError
    end
    short = ArrayData(IntType(64, true), 10,
        [AC._databuffer(UInt8[0xff]), BufferSlice()])
    @assert try
        to_c_data(Field("short", IntType(64, true)), short)
        false
    catch e
        e isa ValidationError
    end
    @assert _registry_count() == before
    println("failed exports leave no registry roots ✓")

    # C strings cannot represent embedded NULs, and Utf8 arrays require
    # valid UTF-8. Reject both before any export root becomes visible.
    badname = Field("embedded\0nul", IntType(64, true); nullable=false)
    @assert try
        to_c_data(badname, md)
        false
    catch e
        e isa ValidationError
    end
    badutf8type = Utf8Type(false)
    badutf8field = Field("bad-utf8", badutf8type)
    badutf8data = ArrayData(badutf8type, 1,
        [BufferSlice(), AC._databuffer(Int32[0, 1]),
         AC._databuffer(UInt8[0xff])]; nullcount=0)
    @assert try
        to_c_data(badutf8field, badutf8data)
        false
    catch e
        e isa ValidationError
    end
    @assert _registry_count() == before
    println("unrepresentable names and invalid UTF-8 fail before export ✓")

    # Dictionary values have independent nullability. Ordered state is a C
    # schema flag, and a non-nullable index may select a null pool value.
    vf, vd = fromjulia("dict", Union{Missing,String}[missing, "x"])
    dt = DictionaryType(IntType(32, true), vf.type, true)
    df = Field("dict", dt; nullable=false, children=vf.children)
    dd = ArrayData(dt, 2,
        [BufferSlice(), AC._databuffer(Int32[0, 1])];
        dictionary=vd, nullcount=0)
    sp, ap = to_c_data(df, dd)
    @assert (unsafe_load(sp).flags & ARROW_FLAG_DICTIONARY_ORDERED) != 0
    df2, dd2 = from_c_data(sp, ap)
    @assert (df2.type::DictionaryType).ordered
    @assert isequal(materialize(df2, dd2), [missing, "x"])
    release!(dd2.owner::ForeignOwner)
    @assert reap!() == 2
    println("dictionary ordered flag and nullable pool values round-trip ✓")

    sp, ap = to_c_data(df, dd)
    sdict = unsafe_load(sp).dictionary
    adict = unsafe_load(ap).dictionary
    smoved = Ref(unsafe_load(sdict))
    amoved = Ref(unsafe_load(adict))
    _store_field!(sdict, :release, Ptr{Cvoid}(C_NULL))
    _store_field!(adict, :release, Ptr{Cvoid}(C_NULL))
    _call_release(sp)
    _call_release(ap)
    @assert reap!() == 0
    GC.@preserve smoved amoved begin
        movedf, movedd = from_c_data(
            Base.unsafe_convert(Ptr{CArrowSchema}, smoved),
            Base.unsafe_convert(Ptr{CArrowArray}, amoved))
        @assert isequal(materialize(movedf, movedd), [missing, "x"])
        release!(movedd.owner::ForeignOwner)
    end
    @assert reap!() == 2
    println("moved dictionaries retain aggregate ownership until release ✓")

    kf, kd = fromjulia("key", ["a"])
    mvf, mvd = fromjulia("value", Int64[7])
    entriesf = Field("entries", StructType(); nullable=false,
        children=[kf, mvf])
    entriesd = ArrayData(StructType(), 1, [BufferSlice()];
        children=[kd, mvd], nullcount=0)
    mt = MapType(true)
    mapf = Field("map", mt; children=[entriesf])
    mapd = ArrayData(mt, 1,
        [BufferSlice(), AC._databuffer(Int32[0, 1])];
        children=[entriesd], nullcount=0)
    sp, ap = to_c_data(mapf, mapd)
    @assert (unsafe_load(sp).flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0
    mapf2, mapd2 = from_c_data(sp, ap)
    @assert (mapf2.type::MapType).keyssorted
    @assert materialize(mapf2, mapd2) == [["a" => 7]]
    release!(mapd2.owner::ForeignOwner)
    @assert reap!() == 2
    println("map sorted-key flag round-trips ✓")

    # Moving a nested subtree keeps all of its descendants live. Releasing
    # the moved entries struct recursively releases its key/value children.
    sp, ap = to_c_data(mapf, mapd)
    sentries = unsafe_load(unsafe_load(sp).children, 1)
    aentries = unsafe_load(unsafe_load(ap).children, 1)
    smoved = Ref(unsafe_load(sentries))
    amoved = Ref(unsafe_load(aentries))
    _store_field!(sentries, :release, Ptr{Cvoid}(C_NULL))
    _store_field!(aentries, :release, Ptr{Cvoid}(C_NULL))
    _call_release(sp)
    _call_release(ap)
    @assert reap!() == 0
    GC.@preserve smoved amoved begin
        movedf, movedd = from_c_data(
            Base.unsafe_convert(Ptr{CArrowSchema}, smoved),
            Base.unsafe_convert(Ptr{CArrowArray}, amoved))
        @assert materialize(movedf, movedd) == [["key" => "a", "value" => 7]]
        release!(movedd.owner::ForeignOwner)
    end
    @assert reap!() == 2
    println("moved nested subtrees retain descendants until release ✓")

    # Two moved siblings keep one aggregate alive. Releasing the first does
    # not free either tree; the second release performs the single reap.
    af, ad = fromjulia("a", Int64[1, 2])
    bf, bd = fromjulia("b", Int64[3, 4])
    sf = Field("s", StructType(); children=[af, bf])
    sd = ArrayData(StructType(), 2, [BufferSlice()];
        children=[ad, bd], nullcount=0)
    sp, ap = to_c_data(sf, sd)
    smoved = Ref{CArrowSchema}[]
    amoved = Ref{CArrowArray}[]
    for i = 1:2
        source_s = unsafe_load(unsafe_load(sp).children, i)
        source_a = unsafe_load(unsafe_load(ap).children, i)
        push!(smoved, Ref(unsafe_load(source_s)))
        push!(amoved, Ref(unsafe_load(source_a)))
        _store_field!(source_s, :release, Ptr{Cvoid}(C_NULL))
        _store_field!(source_a, :release, Ptr{Cvoid}(C_NULL))
    end
    _call_release(sp)
    _call_release(ap)
    @assert reap!() == 0
    for (i, expected_values) in enumerate(([1, 2], [3, 4]))
        GC.@preserve smoved amoved begin
            movedf, movedd = from_c_data(
                Base.unsafe_convert(Ptr{CArrowSchema}, smoved[i]),
                Base.unsafe_convert(Ptr{CArrowArray}, amoved[i]))
            @assert materialize(movedf, movedd) == expected_values
            release!(movedd.owner::ForeignOwner)
        end
        @assert reap!() == (i == 2 ? 2 : 0)
    end
    println("multiple moved siblings defer one aggregate reap ✓")

    # Even when every imported buffer pointer is NULL, ArrayData owns the
    # ForeignOwner. GC cannot release the producer while the empty array lives.
    ef, ed = fromjulia("empty", Int64[])
    sp, ap = to_c_data(ef, ed)
    ef2, ed2 = from_c_data(sp, ap)
    @assert reap!() == 1                    # schema only
    ownerref = WeakRef(ed2.owner)
    GC.gc(true)
    @assert ownerref.value !== nothing
    @assert _registry_count() == 1          # array producer still rooted
    @assert isempty(materialize(ef2, ed2))
    release!(ed2.owner::ForeignOwner)
    @assert reap!() == 1
    println("empty imports retain their shared foreign owner ✓")

    # Natural collection of a forgotten imported tree is also an exactly-once
    # release path: the ForeignOwner finalizer runs the producer callback, so
    # the export root becomes reapable without any caller calling release!.
    ff, fd = fromjulia("finalized", Int64[1])
    sp, ap = to_c_data(ff, fd)
    _import_and_forget(sp, ap)
    finalized_reaped = reap!()
    for _ = 1:10
        finalized_reaped == 2 && break
        GC.gc(true)
        yield()   # let queued finalizer work drain before rescanning
        finalized_reaped += reap!()
    end
    @assert finalized_reaped == 2
    @assert _registry_count() == 0
    println("natural foreign-owner finalization releases the producer ✓")

    # Verifiable C structural failures are clean errors and still release
    # both moved lifetimes exactly once.
    bf, bd = fromjulia("bad", Int64[1])
    sp, ap = to_c_data(bf, bd)
    _store_field!(ap, :buffers, Ptr{Ptr{Cvoid}}(C_NULL))
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError
    end
    @assert reap!() == 2
    @assert _registry_count() == 0
    println("invalid C pointer tables fail with exact cleanup ✓")

    # Flags carry schema semantics, so the importer must reject unknown bits
    # and known flags on layouts where those meanings do not apply. Silent
    # acceptance would discard information that this adapter cannot preserve.
    _expect_invalid_schema_flags!(Int64(8))
    _expect_invalid_schema_flags!(ARROW_FLAG_DICTIONARY_ORDERED)
    _expect_invalid_schema_flags!(ARROW_FLAG_MAP_KEYS_SORTED)
    println("unknown and type-invalid schema flags fail with exact cleanup ✓")

    # A failed import invokes producer callbacks after it has copied the
    # caller-visible structs. Cleanup must therefore use the topology that the
    # producer recorded at export time. Otherwise a NULL child table crashes
    # the callback, while a forged zero child count strands descendants in
    # the registry forever. Cover both schema and array roots.
    _expect_invalid_list_topology!() do _sp, ap
        _store_field!(ap, :children, Ptr{Ptr{CArrowArray}}(C_NULL))
    end
    _expect_invalid_list_topology!() do sp, _ap
        _store_field!(sp, :children, Ptr{Ptr{CArrowSchema}}(C_NULL))
    end
    _expect_invalid_list_topology!() do _sp, ap
        _store_field!(ap, :n_children, Int64(0))
    end
    _expect_invalid_list_topology!() do sp, _ap
        _store_field!(sp, :n_children, Int64(0))
    end
    _expect_invalid_dictionary_topology!() do _sp, ap
        _store_field!(ap, :dictionary, Ptr{CArrowArray}(C_NULL))
    end
    _expect_invalid_dictionary_topology!() do sp, _ap
        _store_field!(sp, :dictionary, Ptr{CArrowSchema}(C_NULL))
    end
    println("malformed public topology cannot corrupt producer cleanup ✓")

    # Imported C names and Utf8 buffers receive the same full validation.
    # Both failures happen after the array move, so both producer lifetimes
    # must still be released exactly once.
    nf, nd = fromjulia("name", Int64[1])
    sp, ap = to_c_data(nf, nd)
    unsafe_store!(unsafe_load(sp).name, 0xff, 1)
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError
    end
    @assert reap!() == 2
    @assert _registry_count() == 0

    uf, ud = fromjulia("utf8", ["a"])
    sp, ap = to_c_data(uf, ud)
    datap = Ptr{UInt8}(unsafe_load(unsafe_load(ap).buffers, 3))
    unsafe_store!(datap, 0xff, 1)
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError
    end
    @assert reap!() == 2
    @assert _registry_count() == 0
    println("invalid imported names and UTF-8 fail with exact cleanup ✓")

    # ---- C stream interface --------------------------------------------

    # Export a two-batch stream through a caller-owned struct, move it into
    # an importer, and compare both batches against the source. Every
    # get_schema/get_next result is its own export root; the stream root
    # itself lives in the stream registry until release.
    sbefore = _registry_count()
    stbefore = _stream_registry_count()
    b1 = batch((xs=Int64[1, 2, 3], strs=["a", missing, "c"]))
    b2 = batch((xs=Int64[4, 5], strs=[missing, "e"]))

    # Stream export owns its control allocation before the next fallible
    # operation. Key overflow and final publication failure must both return
    # that allocation and leave no registry entry.
    stream_deallocations = Ref(0)
    stream_deallocate! = p -> begin
        stream_deallocations[] += 1
        Libc.free(p)
    end
    streamtxnref = Ref{CArrowArrayStream}()
    savedkey = NEXT_KEY[]
    try
        NEXT_KEY[] = typemax(Int64)
        GC.@preserve streamtxnref begin
            streamtxnp = Base.unsafe_convert(Ptr{CArrowArrayStream}, streamtxnref)
            @assert try
                _export_stream!(streamtxnp, b1.schema, AC.RecordBatch[],
                    Libc.malloc, stream_deallocate!, unsafe_store!)
                false
            catch e
                e isa OverflowError
            end
        end
    finally
        NEXT_KEY[] = savedkey
    end
    @assert stream_deallocations[] == 1
    @assert _stream_registry_count() == stbefore
    stream_deallocations[] = 0
    GC.@preserve streamtxnref begin
        streamtxnp = Base.unsafe_convert(Ptr{CArrowArrayStream}, streamtxnref)
        @assert try
            _export_stream!(streamtxnp, b1.schema, AC.RecordBatch[],
                Libc.malloc, stream_deallocate!,
                (_p, _stream) -> error("injected stream publication failure"))
            false
        catch e
            e isa ErrorException &&
                e.msg == "injected stream publication failure"
        end
    end
    @assert stream_deallocations[] == 1
    @assert _stream_registry_count() == stbefore
    println("failed stream export handoffs return control and registry roots ✓")

    # A result root is registered before its C struct is copied into the
    # caller-owned output slot. If that final copy fails, the consumer owns
    # nothing: discard the unpublished root immediately. A failed get_next
    # must also leave the batch available for a later retry.
    resulttxnref = Ref{CArrowArrayStream}()
    schemaout = Ref(CArrowSchema(Ptr{UInt8}(C_NULL), Ptr{UInt8}(C_NULL),
        Ptr{UInt8}(C_NULL), 0, 0, Ptr{Ptr{CArrowSchema}}(C_NULL),
        Ptr{CArrowSchema}(C_NULL), Ptr{Cvoid}(C_NULL), Ptr{Cvoid}(C_NULL)))
    arrayout = Ref(CArrowArray(0, 0, 0, 0, 0,
        Ptr{Ptr{Cvoid}}(C_NULL), Ptr{Ptr{CArrowArray}}(C_NULL),
        Ptr{CArrowArray}(C_NULL), Ptr{Cvoid}(C_NULL), Ptr{Cvoid}(C_NULL)))
    fail_result_publish! = (_out, _result) ->
        error("injected stream result publication failure")
    GC.@preserve resulttxnref schemaout arrayout begin
        resulttxnp = Base.unsafe_convert(Ptr{CArrowArrayStream}, resulttxnref)
        schemaoutp = Base.unsafe_convert(Ptr{CArrowSchema}, schemaout)
        arrayoutp = Base.unsafe_convert(Ptr{CArrowArray}, arrayout)
        export_stream!(resulttxnp, b1.schema, AC.RecordBatch[b1])
        resultstate, _ = _stream_state(resulttxnp)
        resultroots = _registry_count()

        @assert _stream_get_schema_impl(resulttxnp, schemaoutp,
            fail_result_publish!) == EINVAL
        @assert _registry_count() == resultroots

        @assert resultstate.nextindex == 1
        @assert _stream_get_next_impl(resulttxnp, arrayoutp,
            fail_result_publish!) == EINVAL
        @assert _registry_count() == resultroots
        @assert resultstate.nextindex == 1

        @assert _stream_get_next_impl(resulttxnp, arrayoutp,
            unsafe_store!) == 0
        @assert arrayout[].release != C_NULL
        @assert arrayout[].length == b1.nrows
        @assert resultstate.nextindex == 2
        @assert _registry_count() == resultroots + 1
        _release_c_array!(arrayoutp, arrayout[])
        callbacks = resulttxnref[]
        ccall(callbacks.release, Cvoid, (Ptr{CArrowArrayStream},), resulttxnp)
    end
    @assert reap!() == 1
    @assert _registry_count() == sbefore
    @assert _stream_registry_count() == stbefore
    println("failed stream result publication cleans roots and permits retry ✓")

    # Every exported callback closes its C exception boundary. Error-message
    # allocation failure clears the previous message instead of reporting it
    # for the new operation. The mandatory get_last_error callback is checked
    # before a foreign stream is moved.
    callbackref = Ref{CArrowArrayStream}()
    GC.@preserve callbackref begin
        callbackp = Base.unsafe_convert(Ptr{CArrowArrayStream}, callbackref)
        export_stream!(callbackp, b1.schema, AC.RecordBatch[])
        callbackstate, _ = _stream_state(callbackp)
        _set_stream_error!(callbackstate, "old error")
        @assert callbackstate.lasterror != C_NULL
        _set_stream_error!(callbackstate, "new error",
            _ -> Ptr{Cvoid}(C_NULL), Libc.free)
        @assert callbackstate.lasterror == C_NULL
        callbacks = callbackref[]
        @assert ccall(callbacks.get_schema, Cint,
            (Ptr{CArrowArrayStream}, Ptr{CArrowSchema}),
            callbackp, Ptr{CArrowSchema}(C_NULL)) == EINVAL
        errorp = ccall(callbacks.get_last_error, Ptr{UInt8},
            (Ptr{CArrowArrayStream},), callbackp)
        @assert errorp != C_NULL
        @assert occursin("output pointer is NULL", unsafe_string(errorp))
        @assert ccall(callbacks.get_next, Cint,
            (Ptr{CArrowArrayStream}, Ptr{CArrowArray}),
            callbackp, Ptr{CArrowArray}(C_NULL)) == EINVAL
        @assert ccall(callbacks.get_last_error, Ptr{UInt8},
            (Ptr{CArrowArrayStream},), Ptr{CArrowArrayStream}(C_NULL)) == C_NULL
        ccall(callbacks.release, Cvoid, (Ptr{CArrowArrayStream},),
            Ptr{CArrowArrayStream}(C_NULL))
        _store_field!(callbackp, :get_last_error, Ptr{Cvoid}(C_NULL))
        @assert try
            from_c_stream(callbackp)
            false
        catch e
            e isa ArgumentError
        end
        ccall(callbacks.release, Cvoid, (Ptr{CArrowArrayStream},), callbackp)
    end
    @assert _stream_registry_count() == stbefore
    println("stream callbacks close errors and required callbacks are enforced ✓")

    # Finalizer registration happens before the stream move. A failure after
    # registration frees only the inert copy; the source remains the sole
    # live stream and its later release drops the registry root exactly once.
    ownerfailref = Ref{CArrowArrayStream}()
    GC.@preserve ownerfailref begin
        ownerfailp = Base.unsafe_convert(Ptr{CArrowArrayStream}, ownerfailref)
        export_stream!(ownerfailp, b1.schema, AC.RecordBatch[])
        captured_stream_owner = Ref{Any}(nothing)
        stream_failing_registrar = (f, o) -> begin
            captured_stream_owner[] = o
            finalizer(f, o)
            error("injected stream finalizer registration failure")
        end
        @assert try
            StreamOwner(ownerfailref[], stream_failing_registrar)
            false
        catch e
            e isa ErrorException &&
                e.msg == "injected stream finalizer registration failure"
        end
        failed_stream_owner = captured_stream_owner[]::StreamOwner
        @assert (@atomic failed_stream_owner.released)
        @assert ownerfailref[].release != C_NULL
        @assert _stream_registry_count() == stbefore + 1
        finalize(failed_stream_owner)
        release!(failed_stream_owner)
        @assert ownerfailref[].release != C_NULL
        ccall(ownerfailref[].release, Cvoid, (Ptr{CArrowArrayStream},), ownerfailp)
    end
    @assert _stream_registry_count() == stbefore
    println("failed stream-owner finalizer handoff leaves the source live ✓")

    # get_next has already transferred its result when a ForeignOwner
    # constructor runs. If registration fails, release that still-live output
    # slot rather than stranding the batch export root.
    batchfailref = Ref{CArrowArrayStream}()
    GC.@preserve batchfailref begin
        batchfailp = Base.unsafe_convert(Ptr{CArrowArrayStream}, batchfailref)
        export_stream!(batchfailp, b1.schema, AC.RecordBatch[b1])
        batchfailstream = from_c_stream(batchfailp)
        captured_batch_owner = Ref{Any}(nothing)
        batch_owner_factory = arr -> ForeignOwner(arr, (f, o) -> begin
            captured_batch_owner[] = o
            finalizer(f, o)
            error("injected batch-owner finalizer registration failure")
        end)
        @assert try
            _nextbatch!(batchfailstream, batch_owner_factory)
            false
        catch e
            e isa ErrorException &&
                e.msg == "injected batch-owner finalizer registration failure"
        end
        failed_batch_owner = captured_batch_owner[]::ForeignOwner
        @assert (@atomic failed_batch_owner.released)
        finalize(failed_batch_owner)
        release!(failed_batch_owner)
        release!(batchfailstream)
    end
    @assert reap!() == 2                    # schema result + failed batch result
    @assert _registry_count() == sbefore
    @assert _stream_registry_count() == stbefore
    println("failed pulled-batch owner handoff releases its live result ✓")

    streamref = Ref{CArrowArrayStream}()
    GC.@preserve streamref begin
        spp = Base.unsafe_convert(Ptr{CArrowArrayStream}, streamref)
        export_stream!(spp, b1.schema, AC.RecordBatch[b1, b2])
        @assert _stream_registry_count() == stbefore + 1
        s = from_c_stream(spp)
        @assert streamref[].release == C_NULL      # moved out of the source
        @assert length(s.schema.fields) == 2
        @assert [f.name for f in s.schema.fields] == ["xs", "strs"]
        owners = ForeignOwner[]
        for source in (b1, b2)
            got = nextbatch!(s)
            @assert got isa AC.RecordBatch
            @assert got.nrows == source.nrows
            for (i, f) in enumerate(s.schema.fields)
                @assert isequal(collect(Any, materialize(f, got.columns[i])),
                    collect(Any, materialize(source.schema.fields[i],
                        source.columns[i]))) f.name
            end
            push!(owners, got.columns[1].owner::ForeignOwner)
        end
        @assert nextbatch!(s) === nothing
        @assert nextbatch!(s) === nothing          # end of stream is sticky
        release!(s)
        release!(s)                                 # exactly-once
        @assert try
            nextbatch!(s)
            false
        catch e
            e isa ArgumentError
        end
        foreach(release!, owners)
    end
    @assert reap!() == 3                            # one schema + two batch roots
    @assert _registry_count() == sbefore
    @assert _stream_registry_count() == stbefore
    println("C stream export/import round-trips with exact lifecycle ✓")

    # Producer-side failures surface through get_last_error: batch two is
    # invalid UTF-8, so its get_next reports EINVAL and the importer throws
    # a ValidationError carrying the producer's message.
    okf, okd = fromjulia("s", ["ok"])
    badd = ArrayData(Utf8Type(false), 1,
        [BufferSlice(), AC._databuffer(Int32[0, 1]),
         AC._databuffer(UInt8[0xff])]; nullcount=0)
    badsch = Schema(Field[okf])
    streamref2 = Ref{CArrowArrayStream}()
    GC.@preserve streamref2 begin
        spp2 = Base.unsafe_convert(Ptr{CArrowArrayStream}, streamref2)
        export_stream!(spp2, badsch, AC.RecordBatch[
            AC.RecordBatch(badsch, ArrayData[okd], 1),
            AC.RecordBatch(badsch, ArrayData[badd], 1)])
        s2 = from_c_stream(spp2)
        first = nextbatch!(s2)
        @assert first isa AC.RecordBatch
        caught = try
            nextbatch!(s2)
            false
        catch e
            e isa ValidationError && occursin("UTF-8", e.msg)
        end
        @assert caught
        release!(s2)
        release!(first.columns[1].owner::ForeignOwner)
    end
    @assert reap!() == 2                            # schema + first batch root
    @assert _registry_count() == sbefore
    @assert _stream_registry_count() == stbefore
    println("producer errors travel through get_last_error into clean throws ✓")

    # Zero-batch streams end immediately; a moved source cannot be imported
    # twice; releasing the producer side directly leaves importer calls
    # failing cleanly rather than crashing.
    streamref3 = Ref{CArrowArrayStream}()
    GC.@preserve streamref3 begin
        spp3 = Base.unsafe_convert(Ptr{CArrowArrayStream}, streamref3)
        export_stream!(spp3, b1.schema, AC.RecordBatch[])
        s3 = from_c_stream(spp3)
        @assert try
            from_c_stream(spp3)
            false
        catch e
            e isa ArgumentError
        end
        @assert nextbatch!(s3) === nothing
        release!(s3)
    end
    @assert reap!() == 1                            # the get_schema root
    @assert _stream_registry_count() == stbefore
    @assert _registry_count() == sbefore
    println("zero-batch streams, double import, and release edges hold ✓")

    childscript = joinpath(@__DIR__, "cdata_stress_child.jl")
    stresscmd = `$(Base.julia_cmd()) --startup-file=no --threads=4 --project=$(Base.active_project()) $childscript`
    success(stresscmd) || error("threaded C Data stress failed")
    println("threaded C Data stress passed in a four-thread child ✓")

    println()
    println("C Data ownership and round-trip checks passed.")
end
