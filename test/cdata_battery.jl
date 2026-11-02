function cdata_battery()
    if Sys.WORD_SIZE == 64
        @assert sizeof(CArrowSchema) == 72
        @assert fieldoffset.(Ref(CArrowSchema), 1:9) == 0:8:64
        @assert sizeof(CArrowArray) == 80
        @assert fieldoffset.(Ref(CArrowArray), 1:10) == 0:8:72
    elseif Sys.WORD_SIZE == 32
        if Base.datatype_alignment(Int64) == 4 # i686 SysV ABI
            @assert sizeof(CArrowSchema) == 44
            @assert fieldoffset.(Ref(CArrowSchema), 1:9) ==
                    [0, 4, 8, 12, 20, 28, 32, 36, 40]
            @assert sizeof(CArrowArray) == 60
            @assert fieldoffset.(Ref(CArrowArray), 1:10) ==
                    [0, 8, 16, 24, 32, 40, 44, 48, 52, 56]
        else # 32-bit ABIs that align int64_t to 8 bytes
            @assert sizeof(CArrowSchema) == 48
            @assert fieldoffset.(Ref(CArrowSchema), 1:9) ==
                    [0, 4, 8, 16, 24, 32, 36, 40, 44]
            @assert sizeof(CArrowArray) == 64
            @assert fieldoffset.(Ref(CArrowArray), 1:10) ==
                    [0, 8, 16, 24, 32, 40, 44, 48, 52, 56]
        end
    else
        error("unsupported pointer width $(Sys.WORD_SIZE)")
    end
    println("C ABI size and field-offset gate passed for $(Sys.WORD_SIZE)-bit ✓")

    # The export, not the caller's Julia variables, keeps the source column
    # and its buffers alive while the consumer holds raw C pointers.
    before = _drained()
    sp, ap, dataref, regionref = _export_and_forget()
    GC.gc(true)
    @assert dataref.value !== nothing
    @assert regionref.value !== nothing
    rootedf, rootedd = from_c_data(sp, ap)
    @assert materialize(rootedf, rootedd) == [1, 2]
    @assert _drained() == before + 1
    release!(rootedd.owner::ForeignOwner)
    @assert _drained() == before
    println("an export roots its Julia sources across GC ✓")

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
    @assert nlive == length(imported)

    # The consumer's release (here the imported columns' owners) marks each
    # export; the reaper frees it, once, and a second release is inert.
    for (_, d2) in imported
        release!(d2.owner::ForeignOwner)
    end
    @assert _drained() == 0
    for (_, d2) in imported
        release!(d2.owner::ForeignOwner)
    end
    @assert _reap!() == 0
    println("consumer releases free each export once; double release is inert ✓")

    # Releasing an import revokes every region over it, so touching a slice
    # afterwards is a clean InvalidStateException rather than the C Data
    # spec's post-release undefined behavior.
    for (f2, d2) in imported
        @assert (@atomic (d2.owner::ForeignOwner).cell.closed)
        isempty(d2.buffers) && continue
        slice = d2.buffers[end]
        slice.region === nothing && continue
        @assert try
            AC.loadat(slice, UInt8, Int64(0))
            false
        catch e
            e isa InvalidStateException
        end
    end
    println("released owners are revoked ✓")

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
    nestedinnerf = Field("values", RunEndEncodedType(); children=[nestedirf, nestedivf])
    nestedinnerd = ArrayData(
        RunEndEncodedType(),
        2,
        BufferSlice[];
        children=[nestedird, nestedivd],
        nullcount=0,
    )
    nestedorf, nestedord = fromjulia("run_ends", Int32[2, 4])
    paritycases = Tuple{Field,ArrayData}[
        (
            Field("dec128", DecimalType(38, 10, 128)),
            ArrayData(
                DecimalType(38, 10, 128),
                2,
                [BufferSlice(), AC._databuffer(Int128[123, -456])];
                nullcount=0,
            ),
        ),
        (
            Field("dec32", DecimalType(9, 2, 32)),
            ArrayData(
                DecimalType(9, 2, 32),
                2,
                [BufferSlice(), AC._databuffer(Int32[1234, -5678])];
                nullcount=0,
            ),
        ),
        (
            Field("date32", DateType(AC.DAY)),
            ArrayData(
                DateType(AC.DAY),
                2,
                [BufferSlice(), AC._databuffer(Int32[0, 19000])];
                nullcount=0,
            ),
        ),
        (
            Field("date64", DateType(AC.MILLISECOND_DATE)),
            ArrayData(
                DateType(AC.MILLISECOND_DATE),
                2,
                [BufferSlice(), AC._databuffer(Int64[0, 86_400_000])];
                nullcount=0,
            ),
        ),
        (
            Field("time32s", TimeType(AC.SECOND, 32)),
            ArrayData(
                TimeType(AC.SECOND, 32),
                2,
                [BufferSlice(), AC._databuffer(Int32[0, 86_399])];
                nullcount=0,
            ),
        ),
        (
            Field("time64n", TimeType(AC.NANOSECOND, 64)),
            ArrayData(
                TimeType(AC.NANOSECOND, 64),
                2,
                [BufferSlice(), AC._databuffer(Int64[0, 12_345])];
                nullcount=0,
            ),
        ),
        (
            Field("ts-utc", tsnulls),
            ArrayData(
                tsnulls,
                3,
                [AC._databuffer(UInt8[0x05]), AC._databuffer(Int64[7, 0, 9])];
                nullcount=1,
            ),
        ),
        (
            Field("ts-naive", TimestampType(AC.SECOND, nothing)),
            ArrayData(
                TimestampType(AC.SECOND, nothing),
                1,
                [BufferSlice(), AC._databuffer(Int64[42])];
                nullcount=0,
            ),
        ),
        (
            Field("dur", DurationType(AC.MILLISECOND)),
            ArrayData(
                DurationType(AC.MILLISECOND),
                2,
                [BufferSlice(), AC._databuffer(Int64[5, -5])];
                nullcount=0,
            ),
        ),
        (
            Field("iym", IntervalType(AC.YEAR_MONTH)),
            ArrayData(
                IntervalType(AC.YEAR_MONTH),
                2,
                [BufferSlice(), AC._databuffer(Int32[12, -1])];
                nullcount=0,
            ),
        ),
        (
            Field("idt", IntervalType(AC.DAY_TIME)),
            ArrayData(
                IntervalType(AC.DAY_TIME),
                2,
                [BufferSlice(), AC._databuffer(Int32[1, 2, 3, 4])];
                nullcount=0,
            ),
        ),
        (
            Field("imdn", IntervalType(AC.MONTH_DAY_NANO)),
            ArrayData(
                IntervalType(AC.MONTH_DAY_NANO),
                1,
                [
                    BufferSlice(),
                    AC._databuffer(
                        vcat(reinterpret(UInt8, Int32[1, 2]), reinterpret(UInt8, Int64[3])),
                    ),
                ];
                nullcount=0,
            ),
        ),
        (
            Field("fsb", FixedSizeBinaryType(3)),
            ArrayData(
                FixedSizeBinaryType(3),
                2,
                [BufferSlice(), AC._databuffer(collect(codeunits("abcdef")))];
                nullcount=0,
            ),
        ),
        (
            Field("fsl", FixedSizeListType(2); children=[fslu]),
            ArrayData(
                FixedSizeListType(2),
                2,
                [BufferSlice()];
                children=[fromjulia("fsl-child", Int64[1, 2, 3, 4])[2]],
                nullcount=0,
            ),
        ),
        (
            Field("lu", Utf8Type(true)),
            ArrayData(
                Utf8Type(true),
                2,
                [
                    BufferSlice(),
                    AC._databuffer(Int64[0, 1, 3]),
                    AC._databuffer(collect(codeunits("abc"))),
                ];
                nullcount=0,
            ),
        ),
        (
            Field("lz", BinaryType(true)),
            ArrayData(
                BinaryType(true),
                2,
                [
                    BufferSlice(),
                    AC._databuffer(Int64[0, 2, 3]),
                    AC._databuffer(UInt8[0x01, 0x02, 0x03]),
                ];
                nullcount=0,
            ),
        ),
        (
            Field("ll", ListType(true); children=[fslu]),
            ArrayData(
                ListType(true),
                2,
                [BufferSlice(), AC._databuffer(Int64[0, 2, 4])];
                children=[fromjulia("fsl-child", Int64[1, 2, 3, 4])[2]],
                nullcount=0,
            ),
        ),
        (
            Field("su", sut; nullable=false, children=[sui, sus]),
            ArrayData(
                sut,
                3,
                [AC._databuffer(Int8[0, 1, 0])];
                children=[sud, susd],
                nullcount=0,
            ),
        ),
        (
            Field("du", dut; nullable=false, children=[dui, dus]),
            ArrayData(
                dut,
                3,
                [AC._databuffer(Int8[0, 1, 0]), AC._databuffer(Int32[0, 0, 1])];
                children=[duid, dusd],
                nullcount=0,
            ),
        ),
        (Field("nulls", NullType()), ArrayData(NullType(), 3, BufferSlice[]; nullcount=3)),
        # format 1.3/1.4: views (with the C-only trailing sizes buffer),
        # list-views (per-slot offsets+sizes, unordered/overlapping), REE
        (
            Field("vu", ViewType(true); nullable=true),
            ArrayData(
                ViewType(true),
                3,
                [
                    AC._databuffer(UInt8[0x05]),
                    AC._databuffer(
                        vcat(
                            _viewentry(3, collect(codeunits("abc"))),
                            _viewlong(25, collect(codeunits("firs")), 0, 0),
                            _viewlong(26, collect(codeunits("seco")), 1, 0),
                        ),
                    ),
                    AC._databuffer(collect(codeunits("first-out-of-line-payload"))),
                    AC._databuffer(collect(codeunits("second-buffer-payload-here"))),
                ];
                nullcount=1,
            ),
        ),
        (
            Field("vz", ViewType(false)),
            ArrayData(
                ViewType(false),
                1,
                [BufferSlice(), AC._databuffer(_viewentry(2, UInt8[0xff, 0x00]))];
                nullcount=0,
            ),
        ),
        (
            Field("lv", ListViewType(false); children=[fslu]),
            ArrayData(
                ListViewType(false),
                3,
                [
                    BufferSlice(),
                    AC._databuffer(Int32[2, 0, 0]),
                    AC._databuffer(Int32[2, 2, 4]),
                ];
                children=[fromjulia("fsl-child", Int64[1, 2, 3, 4])[2]],
                nullcount=0,
            ),
        ),
        (
            Field("Lv", ListViewType(true); children=[fslu]),
            ArrayData(
                ListViewType(true),
                1,
                [BufferSlice(), AC._databuffer(Int64[1]), AC._databuffer(Int64[3])];
                children=[fromjulia("fsl-child", Int64[1, 2, 3, 4])[2]],
                nullcount=0,
            ),
        ),
        (
            Field(
                "ree",
                RunEndEncodedType();
                children=[
                    Field("run_ends", IntType(32, true); nullable=false),
                    Field("values", Utf8Type(false); nullable=true),
                ],
            ),
            ArrayData(
                RunEndEncodedType(),
                4,
                BufferSlice[];
                children=[
                    fromjulia("run_ends", Int32[2, 3, 4])[2],
                    fromjulia("values", Union{Missing,String}["x", missing, "z"])[2],
                ],
                nullcount=0,
            ),
        ),
        (
            Field("nested-ree", RunEndEncodedType(); children=[nestedorf, nestedinnerf]),
            ArrayData(
                RunEndEncodedType(),
                4,
                BufferSlice[];
                children=[nestedord, nestedinnerd],
                nullcount=0,
            ),
        ),
    ]
    for (f, d) in paritycases
        want = collect(Any, materialize(f, d))
        sp, ap = to_c_data(f, d)
        f2, d2 = from_c_data(sp, ap)
        @assert AC.typeequal(f2.type, f.type) f.name
        @assert isequal(collect(Any, materialize(f2, d2)), want) f.name
        release!(d2.owner::ForeignOwner)
    end
    @assert _drained() == 0
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
        "v",
        "vx",
        "+v",
        "+vx",
        "+rr",
        "d:x",
        "w:",
        "tsq:",
        "tsé:",
        "ts💣:",
        "tsu:UTC\0hidden",
        "w: 1",
        "w:1 ",
        "w:+1",
        "w:0x10",
        "+w: 2",
        "d: 1,0",
        "d:1, 0",
        "d:+1,+0",
        "d:0x9,0x2,0x20",
        "d:0,0",
        "d:39,0",
        "d:1,0,1",
        "d:77,0,256",
        "+ud:200",
        "+ud:0,0",
        "+ud: 0,1",
        "+us:+1",
        "+ud:0x0,0x1",
        "+ud:" * join(0:128, ","),
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
        push!(
            emptyoffsetcases,
            (
                Field("empty", t),
                ArrayData(t, 0, [BufferSlice(), BufferSlice(), BufferSlice()]; nullcount=0),
            ),
        )
    end
    for t in (ListType(false), ListType(true))
        push!(
            emptyoffsetcases,
            (
                Field("empty-list", t; children=[emptyitemf]),
                ArrayData(
                    t,
                    0,
                    [BufferSlice(), BufferSlice()];
                    children=[emptyitemd],
                    nullcount=0,
                ),
            ),
        )
    end
    emptykeyt = Utf8Type(false)
    emptykeyf = Field("key", emptykeyt; nullable=false)
    emptykeyd =
        ArrayData(emptykeyt, 0, [BufferSlice(), BufferSlice(), BufferSlice()]; nullcount=0)
    emptyvaluef, emptyvalued = fromjulia("value", Int64[])
    emptyentriesf =
        Field("entries", StructType(); nullable=false, children=[emptykeyf, emptyvaluef])
    emptyentriesd = ArrayData(
        StructType(),
        0,
        [BufferSlice()];
        children=[emptykeyd, emptyvalued],
        nullcount=0,
    )
    emptymapt = MapType(false)
    push!(
        emptyoffsetcases,
        (
            Field("empty-map", emptymapt; children=[emptyentriesf]),
            ArrayData(
                emptymapt,
                0,
                [BufferSlice(), BufferSlice()];
                children=[emptyentriesd],
                nullcount=0,
            ),
        ),
    )
    for (f, d) in emptyoffsetcases
        spec = layoutspec(f.type)
        oi = findfirst(==(AC.OFFSETS), spec.buffers)::Int
        sp, ap = to_c_data(f, d)
        arr = unsafe_load(ap)
        offsetp = Ptr{UInt8}(unsafe_load(arr.buffers, oi))
        @assert offsetp != C_NULL
        GC.gc(true)
        @assert spec.offsetwidth == 4 ? unsafe_load(Ptr{Int32}(offsetp)) == 0 :
                unsafe_load(Ptr{Int64}(offsetp)) == 0
        f2, d2 = from_c_data(sp, ap)
        @assert d2.buffers[oi].len == spec.offsetwidth
        @assert isempty(materialize(f2, d2))
        release!(d2.owner::ForeignOwner)
        @assert _drained() == 0
    end
    println("empty C Data offset layouts export one rooted terminal zero ✓")
    nullf = Field("null-empty", Utf8Type(false))
    nulld = ArrayData(
        Utf8Type(false),
        0,
        [BufferSlice(), BufferSlice(), BufferSlice()];
        nullcount=0,
    )
    sp, ap = to_c_data(nullf, nulld)
    unsafe_store!(unsafe_load(ap).buffers, Ptr{Cvoid}(C_NULL), 2)
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError && occursin("NULL OFFSETS buffer", e.msg)
    end
    @assert _drained() == 0
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
    @assert _drained() == 0

    earlyunionf = Field("early-union", sut; children=[sui, sus])
    earlyuniond = ArrayData(
        sut,
        3,
        [AC._databuffer(Int8[0, 1, 0])];
        children=[sud, susd],
        nullcount=0,
    )
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
    @assert _drained() == 0
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
    @assert _drained() == 0
    println("negative C Data final offsets fail cleanly ✓")
    # Lengths and offsets whose required extents overflow Int64 are malformed
    # declarations: ValidationError, never a raw OverflowError.
    for (f, d) in (fromjulia("wide", Int64[1]), fromjulia("text", ["x"]))
        for (len, off) in ((typemax(Int64), 0), (2, typemax(Int64)), (Int64(2)^61, 0))
            sp, ap = to_c_data(f, d)
            _store_field!(ap, :length, len)
            _store_field!(ap, :offset, off)
            @assert try
                from_c_data(sp, ap)
                false
            catch e
                e isa ValidationError
            end
            @assert _drained() == 0
        end
    end
    println("overflowing C Data extents fail as ValidationErrors ✓")
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
    @assert _drained() == 0
    println("moved (released) source cannot be imported twice ✓")

    # A producer whose release callback does not mark the struct released
    # breaks the interface: `release!` reports it, once, and later releases
    # are inert.
    before_calls = TEST_NONCONFORMING_RELEASES[]
    badarr = Ref(_test_c_array(test_nonconforming_release()))
    badowner = GC.@preserve badarr _moveowner(Base.unsafe_convert(Ptr{CArrowArray}, badarr))
    @assert badarr[].release == C_NULL        # moved out of the source
    @assert try
        release!(badowner)
        false
    catch e
        e isa ErrorException &&
            e.msg == "C Data producer release did not mark the structure released"
    end
    @assert TEST_NONCONFORMING_RELEASES[] == before_calls + 1
    release!(badowner)
    finalize(badowner)
    @assert TEST_NONCONFORMING_RELEASES[] == before_calls + 1
    println("nonconforming producer release reports the error once ✓")

    # A root release must transitively release every child. Inspect before
    # reap, while the exported structs remain allocated.
    lf, ld = b.schema.fields[4], b.columns[4]
    sp, ap = to_c_data(lf, ld)
    schild = unsafe_load(unsafe_load(sp).children, 1)
    achild = unsafe_load(unsafe_load(ap).children, 1)
    _release_c!(sp)
    _release_c!(ap)
    @assert unsafe_load(sp).release == C_NULL
    @assert unsafe_load(schild).release == C_NULL
    @assert unsafe_load(ap).release == C_NULL
    @assert unsafe_load(achild).release == C_NULL
    @assert _drained() == 0
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
    _release_c!(sp)
    _release_c!(ap)
    @assert _drained() == 1
    GC.@preserve smoved amoved begin
        smovedp = Base.unsafe_convert(Ptr{CArrowSchema}, smoved)
        amovedp = Base.unsafe_convert(Ptr{CArrowArray}, amoved)
        @assert unsafe_load(smovedp).release != C_NULL
        @assert unsafe_load(amovedp).release != C_NULL
        movedf, movedd = from_c_data(smovedp, amovedp)
        @assert materialize(movedf, movedd) == [1, 2, 3]
        release!(movedd.owner::ForeignOwner)
    end
    @assert _drained() == 0
    println("moved children retain aggregate ownership until release ✓")

    # The C release entrypoints have no error channel: a release marks the
    # struct released, a repeat call on it is inert, and the export is freed
    # once every node has been released.
    rf, rd = fromjulia("plain-release", Int64[1])
    sp, ap = to_c_data(rf, rd)
    _release_c!(ap)
    @assert unsafe_load(ap).release == C_NULL
    _release_c!(ap)   # inert repeat
    @assert _drained() == 1
    _release_c!(sp)
    @assert _drained() == 0
    println("C release entrypoints are exactly-once and repeats are inert ✓")

    # A consumer may read a top-level struct it just released in place (Arrow C++
    # checks that its `release` is NULL), so the reap that frees the export keeps the
    # two top-level structs for 10 ms, however many reaps run meanwhile; 5 ms in, they
    # are still there. Collections are off so no other reap runs.
    GC.enable(false)
    try
        @assert _drained() == 0
        sp, ap = to_c_data(fromjulia("in-place", Int64[1, 2])...)
        key = _exportkey(ap)
        top = Ptr{Cvoid}(sp)
        retired() = any(r -> r[2] == top, _RETIRED)
        _release_c!(sp)
        _release_c!(ap)
        t0 = time_ns()
        @assert _reap!() == 1
        @assert !_registered(key) && retired()
        Libc.systemsleep(0.005)
        _reap!()
        _reap!()
        if time_ns() - t0 < 10_000_000
            @assert retired()
            @assert unsafe_load(ap).release == C_NULL
        end
        Libc.systemsleep(0.011)
        _reap!()
        @assert !retired()
    finally
        GC.enable(true)
    end
    println("to_c_data's top-level structs outlive their export by 10 ms ✓")

    # Consumers call the release callbacks from their own threads and from inside
    # garbage-collector finalizers, so they allocate nothing. (Julia 1.10's call
    # through the pointer of a precompiled `@cfunction` allocates 16 bytes itself;
    # the same body compiled in the session allocates none.)
    _release_allocations()
    VERSION >= v"1.11" && @assert _release_allocations() == 0
    # With nothing exported afterwards, garbage collections free what a
    # consumer released.
    key = _released_export_key()
    for _ = 1:10
        _registered(key) || break
        GC.gc(true)
        yield()
    end
    @assert !_registered(key)
    println("release callbacks allocate nothing; collections free released exports ✓")

    # A consumer that keeps every batch it pulls (pyarrow's `read_all`) pays the
    # same for each export however many it holds: exports on top of 50,000 held
    # ones cost what they cost alone, not a scan of the held ones each.
    before = _drained()
    alone = _held_export_seconds(2_000)
    held = _hold_exports(50_000)
    ontop = _held_export_seconds(2_000)
    _release_all!(held)
    @assert ontop < 5 * alone (ontop, alone)
    @assert _drained() == before
    # Once the held exports are freed the registry shrinks, so later reaps walk only
    # what is registered.
    @assert length(EXPORTS.slots) <= 16 * max(64, _registry_count())
    println("exports cost the same however many the consumer holds ✓")

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
    short = ArrayData(IntType(64, true), 10, [AC._databuffer(UInt8[0xff]), BufferSlice()])
    @assert try
        to_c_data(Field("short", IntType(64, true)), short)
        false
    catch e
        e isa ValidationError
    end
    @assert _registry_count() == before
    println("failed exports leave no registry roots ✓")
    # C strings cannot represent embedded NULs: reject before any export
    # root becomes visible.
    badname = Field("embedded\0nul", IntType(64, true); nullable=false)
    @assert try
        to_c_data(badname, md)
        false
    catch e
        e isa ValidationError
    end
    @assert _registry_count() == before
    # Utf8 content is NOT judged at the boundary — the exporter applies the
    # semantic tier, exactly like the IPC writer. Invalid UTF-8 is what the
    # opt-in `validate_full` tier catches; the export itself succeeds and the
    # bytes cross unchanged.
    badutf8type = Utf8Type(false)
    badutf8field = Field("bad-utf8", badutf8type)
    badutf8data = ArrayData(
        badutf8type,
        1,
        [BufferSlice(), AC._databuffer(Int32[0, 1]), AC._databuffer(UInt8[0xff])];
        nullcount=0,
    )
    @assert try
        validate_full(badutf8field, badutf8data)
        false
    catch e
        e isa ValidationError
    end
    sp, ap = to_c_data(badutf8field, badutf8data)
    rawf, rawd = from_c_data(sp, ap)
    @assert codeunits(getvalue(rawf, rawd, 1)) == UInt8[0xff]
    release!(rawd.owner::ForeignOwner)
    @assert _drained() == 0
    @assert _registry_count() == before
    println("unrepresentable names fail before export; content policy is opt-in ✓")
    # Dictionary values have independent nullability. Ordered state is a C
    # schema flag, and a non-nullable index may select a null pool value.
    vf, vd = fromjulia("dict", Union{Missing,String}[missing, "x"])
    dt = DictionaryType(IntType(32, true), vf.type, true)
    df = Field("dict", dt; nullable=false, children=vf.children)
    dd = ArrayData(
        dt,
        2,
        [BufferSlice(), AC._databuffer(Int32[0, 1])];
        dictionary=vd,
        nullcount=0,
    )
    sp, ap = to_c_data(df, dd)
    @assert (unsafe_load(sp).flags & ARROW_FLAG_DICTIONARY_ORDERED) != 0
    df2, dd2 = from_c_data(sp, ap)
    @assert (df2.type::DictionaryType).ordered
    @assert isequal(materialize(df2, dd2), [missing, "x"])
    release!(dd2.owner::ForeignOwner)
    @assert _drained() == 0
    println("dictionary ordered flag and nullable pool values round-trip ✓")
    sp, ap = to_c_data(df, dd)
    sdict = unsafe_load(sp).dictionary
    adict = unsafe_load(ap).dictionary
    smoved = Ref(unsafe_load(sdict))
    amoved = Ref(unsafe_load(adict))
    _store_field!(sdict, :release, Ptr{Cvoid}(C_NULL))
    _store_field!(adict, :release, Ptr{Cvoid}(C_NULL))
    _release_c!(sp)
    _release_c!(ap)
    @assert _drained() == 1
    GC.@preserve smoved amoved begin
        movedf, movedd = from_c_data(
            Base.unsafe_convert(Ptr{CArrowSchema}, smoved),
            Base.unsafe_convert(Ptr{CArrowArray}, amoved),
        )
        @assert isequal(materialize(movedf, movedd), [missing, "x"])
        release!(movedd.owner::ForeignOwner)
    end
    @assert _drained() == 0
    println("moved dictionaries retain aggregate ownership until release ✓")
    kf, kd = fromjulia("key", ["a"])
    mvf, mvd = fromjulia("value", Int64[7])
    entriesf = Field("entries", StructType(); nullable=false, children=[kf, mvf])
    entriesd = ArrayData(StructType(), 1, [BufferSlice()]; children=[kd, mvd], nullcount=0)
    mt = MapType(true)
    mapf = Field("map", mt; children=[entriesf])
    mapd = ArrayData(
        mt,
        1,
        [BufferSlice(), AC._databuffer(Int32[0, 1])];
        children=[entriesd],
        nullcount=0,
    )
    sp, ap = to_c_data(mapf, mapd)
    @assert (unsafe_load(sp).flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0
    mapf2, mapd2 = from_c_data(sp, ap)
    @assert (mapf2.type::MapType).keyssorted
    @assert materialize(mapf2, mapd2) == [["a" => 7]]
    release!(mapd2.owner::ForeignOwner)
    @assert _drained() == 0
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
    _release_c!(sp)
    _release_c!(ap)
    @assert _drained() == 1
    GC.@preserve smoved amoved begin
        movedf, movedd = from_c_data(
            Base.unsafe_convert(Ptr{CArrowSchema}, smoved),
            Base.unsafe_convert(Ptr{CArrowArray}, amoved),
        )
        @assert materialize(movedf, movedd) == [["key" => "a", "value" => 7]]
        release!(movedd.owner::ForeignOwner)
    end
    @assert _drained() == 0
    println("moved nested subtrees retain descendants until release ✓")
    # Two moved siblings keep one aggregate alive. Releasing the first does
    # not free either tree; the second release performs the single reap.
    af, ad = fromjulia("a", Int64[1, 2])
    bf, bd = fromjulia("b", Int64[3, 4])
    sf = Field("s", StructType(); children=[af, bf])
    sd = ArrayData(StructType(), 2, [BufferSlice()]; children=[ad, bd], nullcount=0)
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
    _release_c!(sp)
    _release_c!(ap)
    @assert _drained() == 1
    for (i, expected_values) in enumerate(([1, 2], [3, 4]))
        GC.@preserve smoved amoved begin
            movedf, movedd = from_c_data(
                Base.unsafe_convert(Ptr{CArrowSchema}, smoved[i]),
                Base.unsafe_convert(Ptr{CArrowArray}, amoved[i]),
            )
            @assert materialize(movedf, movedd) == expected_values
            release!(movedd.owner::ForeignOwner)
        end
        @assert _drained() == (i == 2 ? 0 : 1)
    end
    println("multiple moved siblings defer one aggregate reap ✓")
    # Even when every imported buffer pointer is NULL, ArrayData owns the
    # ForeignOwner. GC cannot release the producer while the empty array lives.
    ef, ed = fromjulia("empty", Int64[])
    sp, ap = to_c_data(ef, ed)
    ef2, ed2 = from_c_data(sp, ap)
    _reap!()
    ownerref = WeakRef(ed2.owner)
    GC.gc(true)
    @assert ownerref.value !== nothing
    @assert _drained() == 1          # the export still roots the source
    @assert isempty(materialize(ef2, ed2))
    release!(ed2.owner::ForeignOwner)
    @assert _drained() == 0
    println("empty imports retain their shared foreign owner ✓")
    # Natural collection of a forgotten imported tree is also an exactly-once
    # release path: the ForeignOwner finalizer runs the producer callback, so
    # the export root becomes reapable without any caller calling release!.
    ff, fd = fromjulia("finalized", Int64[1])
    sp, ap = to_c_data(ff, fd)
    _import_and_forget(sp, ap)
    for _ = 1:10
        _drained() == 0 && break
        GC.gc(true)
        yield()   # let queued finalizer work drain before rescanning
    end
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
    @assert _drained() == 0
    @assert _registry_count() == 0
    println("invalid C pointer tables fail with exact cleanup ✓")
    # Flags carry schema semantics, so the importer must reject unknown bits
    # and known flags on layouts where those meanings do not apply. Silent
    # acceptance would discard information that this adapter cannot preserve.
    _expect_invalid_schema_flags!(Int64(8))
    _expect_invalid_schema_flags!(ARROW_FLAG_DICTIONARY_ORDERED)
    _expect_invalid_schema_flags!(ARROW_FLAG_MAP_KEYS_SORTED)
    println("unknown and type-invalid schema flags fail with exact cleanup ✓")
    # Imported C names must be valid UTF-8 (they become Field names). The
    # failure happens after the array move, so both producer lifetimes must
    # still be released exactly once.
    nf, nd = fromjulia("name", Int64[1])
    sp, ap = to_c_data(nf, nd)
    unsafe_store!(unsafe_load(sp).name, 0xff, 1)
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError
    end
    @assert _drained() == 0
    @assert _registry_count() == 0

    # Imported Utf8 CONTENT is not judged by the importer (semantic tier, the
    # IPC reader's default); the opt-in `validate_full` on the returned pair
    # is where invalid UTF-8 is caught.
    uf, ud = fromjulia("utf8", ["a"])
    sp, ap = to_c_data(uf, ud)
    datap = Ptr{UInt8}(unsafe_load(unsafe_load(ap).buffers, 3))
    unsafe_store!(datap, 0xff, 1)
    uf2, ud2 = from_c_data(sp, ap)
    @assert try
        validate_full(uf2, ud2)
        false
    catch e
        e isa ValidationError
    end
    release!(ud2.owner::ForeignOwner)
    @assert _drained() == 0
    @assert _registry_count() == 0
    println("invalid imported names fail with exact cleanup; content policy is opt-in ✓")

    # ---- C stream interface --------------------------------------------

    # Export a two-batch stream through a caller-owned struct, move it into
    # an importer, and compare both batches against the source. The stream,
    # its get_schema result, and every get_next result are exports of their
    # own, freed once the consumer has released each.
    sbefore = _registry_count()
    b1 = batch((xs=Int64[1, 2, 3], strs=["a", missing, "c"]))
    b2 = batch((xs=Int64[4, 5], strs=[missing, "e"]))

    # Every exported callback closes its C exception boundary: a NULL output
    # slot is EINVAL with the reason in get_last_error, a NULL stream is
    # inert, and an importer refuses a stream missing a required callback.
    callbackref = Ref{CArrowArrayStream}()
    GC.@preserve callbackref begin
        callbackp = Base.unsafe_convert(Ptr{CArrowArrayStream}, callbackref)
        export_stream!(callbackp, b1.schema, AC.RecordBatch[])
        callbacks = callbackref[]
        @assert ccall(
            callbacks.get_last_error,
            Ptr{UInt8},
            (Ptr{CArrowArrayStream},),
            callbackp,
        ) == C_NULL
        @assert ccall(
            callbacks.get_schema,
            Cint,
            (Ptr{CArrowArrayStream}, Ptr{CArrowSchema}),
            callbackp,
            Ptr{CArrowSchema}(C_NULL),
        ) == EINVAL
        errorp = ccall(
            callbacks.get_last_error,
            Ptr{UInt8},
            (Ptr{CArrowArrayStream},),
            callbackp,
        )
        @assert errorp != C_NULL
        @assert occursin("output pointer is NULL", unsafe_string(errorp))
        @assert ccall(
            callbacks.get_next,
            Cint,
            (Ptr{CArrowArrayStream}, Ptr{CArrowArray}),
            callbackp,
            Ptr{CArrowArray}(C_NULL),
        ) == EINVAL
        @assert ccall(
            callbacks.get_last_error,
            Ptr{UInt8},
            (Ptr{CArrowArrayStream},),
            Ptr{CArrowArrayStream}(C_NULL),
        ) == C_NULL
        ccall(
            callbacks.release,
            Cvoid,
            (Ptr{CArrowArrayStream},),
            Ptr{CArrowArrayStream}(C_NULL),
        )
        _store_field!(callbackp, :get_last_error, Ptr{Cvoid}(C_NULL))
        @assert try
            from_c_stream(callbackp)
            false
        catch e
            e isa ArgumentError
        end
        ccall(callbacks.release, Cvoid, (Ptr{CArrowArrayStream},), callbackp)
        @assert callbackref[].release == C_NULL
    end
    @assert _drained() == 0
    @assert _registry_count() == sbefore
    println("stream callbacks close errors and required callbacks are enforced ✓")

    # One task uses an imported stream at a time. Hold one pull open inside
    # the producer: a second pull and a release fail at once, without
    # touching the producer, and the first pull completes afterwards.
    overlapref = Ref{CArrowArrayStream}()
    GC.@preserve overlapref begin
        overlapp = Base.unsafe_convert(Ptr{CArrowArrayStream}, overlapref)
        entered = Channel{Nothing}(1)
        resume = Channel{Nothing}(1)
        rowf, rowd = fromjulia("x", Int64[7, 8])
        rowsf = Field("", StructType(); nullable=false, children=[rowf])
        rowsd = ArrayData(StructType(), 2, [BufferSlice()]; children=[rowd], nullcount=0)
        overlapstream =
            from_c_stream(one_batch_stream!(overlapp, rowsf, rowsd, (entered, resume)))
        firstpull = @async nextbatch!(overlapstream)
        take!(entered)
        @assert try
            nextbatch!(overlapstream)
            false
        catch e
            e isa Base.ConcurrencyViolationError
        end
        @assert try
            release!(overlapstream)
            false
        catch e
            e isa Base.ConcurrencyViolationError
        end
        put!(resume, nothing)
        overlapbatch = fetch(firstpull)
        @assert overlapbatch isa AC.RecordBatch
        @assert materialize(rowf, overlapbatch.columns[1]) == [7, 8]
        @assert nextbatch!(overlapstream) === nothing
        release!(overlapbatch.columns[1].owner::ForeignOwner)
        release!(overlapstream)
    end
    @assert _drained() == 0
    @assert _registry_count() == sbefore
    println("overlapping imported-stream calls fail and the cursor recovers ✓")

    streamref = Ref{CArrowArrayStream}()
    GC.@preserve streamref begin
        spp = Base.unsafe_convert(Ptr{CArrowArrayStream}, streamref)
        export_stream!(spp, b1.schema, AC.RecordBatch[b1, b2])
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
                @assert isequal(
                    collect(Any, materialize(f, got.columns[i])),
                    collect(Any, materialize(source.schema.fields[i], source.columns[i])),
                ) f.name
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
    @assert _drained() == 0
    @assert _registry_count() == sbefore
    println("C stream export/import round-trips with exact lifecycle ✓")
    # Producer-side failures surface through get_last_error: batch two has
    # non-monotonic offsets (a semantic-tier defect that structural
    # construction cannot see), so its get_next reports EINVAL and the
    # importer throws a ValidationError carrying the producer's message.
    okf, okd = fromjulia("s", ["ok"])
    badd = ArrayData(
        Utf8Type(false),
        1,
        [BufferSlice(), AC._databuffer(Int32[1, 0]), AC._databuffer(UInt8[0x61])];
        nullcount=0,
    )
    badsch = Schema(Field[okf])
    streamref2 = Ref{CArrowArrayStream}()
    GC.@preserve streamref2 begin
        spp2 = Base.unsafe_convert(Ptr{CArrowArrayStream}, streamref2)
        export_stream!(
            spp2,
            badsch,
            AC.RecordBatch[
                AC.RecordBatch(badsch, ArrayData[okd], 1),
                AC.RecordBatch(badsch, ArrayData[badd], 1),
            ],
        )
        s2 = from_c_stream(spp2)
        first = nextbatch!(s2)
        @assert first isa AC.RecordBatch
        caught = try
            nextbatch!(s2)
            false
        catch e
            e isa ValidationError && occursin("monotonic", e.msg)
        end
        @assert caught
        release!(s2)
        release!(first.columns[1].owner::ForeignOwner)
    end
    @assert _drained() == 0
    @assert _registry_count() == sbefore
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
    @assert _drained() == 0
    @assert _registry_count() == sbefore
    println("zero-batch streams, double import, and release edges hold ✓")
    # A C stream batch is a struct array. Null struct rows have no record-batch
    # form and must be refused, not dropped; a struct offset and children that
    # run past the struct select the visible child window.
    rowf, rowd = fromjulia("x", Int64[10, 20, 30, 40, 50])
    rowsf = Field("", StructType(); nullable=false, children=[rowf])
    rowsref = Ref{CArrowArrayStream}()
    GC.@preserve rowsref begin
        rowsp = Base.unsafe_convert(Ptr{CArrowArrayStream}, rowsref)
        nullrows = ArrayData(
            StructType(),
            5,
            [AC._bitmapbuffer(Bool[1, 0, 1, 1, 1])];
            children=[rowd],
            nullcount=1,
        )
        rows = from_c_stream(one_batch_stream!(rowsp, rowsf, nullrows))
        @assert try
            nextbatch!(rows)
            false
        catch e
            e isa ValidationError && occursin("null rows", e.msg)
        end
        release!(rows)
        window = ArrayData(
            StructType(),
            2,
            [BufferSlice()];
            offset=2,
            children=[rowd],
            nullcount=0,
        )
        rows = from_c_stream(one_batch_stream!(rowsp, rowsf, window))
        got = nextbatch!(rows)
        @assert got.nrows == 2
        @assert materialize(got.schema.fields[1], got.columns[1]) == Any[30, 40]
        @assert nextbatch!(rows) === nothing
        release!(rows)
        release!(got.columns[1].owner::ForeignOwner)
    end
    @assert _drained() == 0
    @assert _registry_count() == sbefore
    println("C stream struct batches refuse null rows and use their child window ✓")
    # Field metadata crosses the C boundary (the C data interface blob:
    # int32 pair count, length-prefixed keys and values), recursively
    # through children and dictionary value fields; absent metadata stays
    # NULL and imports as nothing.
    mleaf = Field("item", IntType(64, true); nullable=false, metadata=["lk" => "lv"])
    _, mld = fromjulia("l", [Int64[1, 2], Int64[3]])
    mlist = Field(
        "l",
        ListType(false);
        nullable=false,
        metadata=["k" => "v", "empty" => ""],
        children=[mleaf],
    )
    msp, map_ = to_c_data(mlist, mld)
    mf2, mld2 = from_c_data(msp, map_)
    @assert collect(mf2.metadata) == ["k" => "v", "empty" => ""]
    @assert collect(mf2.children[1].metadata) == ["lk" => "lv"]
    @assert getvalue(mf2, mld2, 1) == Any[1, 2]
    release!(mld2.buffers[2].region::OwnerRegion)
    _reap!()
    dvf, dvd = AC.fromjulia_dict("d", ["lo", "hi"], [0, 1, missing])
    dmf = Field(
        "d",
        dvf.type;
        nullable=dvf.nullable,
        metadata=["dk" => "dv"],
        children=collect(Field, dvf.children),
    )
    dsp, dap = to_c_data(dmf, dvd)
    # Dictionary field metadata rides the OUTER wrapper node (the C++
    # bridge exports field metadata there; PyArrow imports only the
    # wrapper's pairs); the dependent value node carries none.
    dsch = unsafe_load(dsp)
    @assert dsch.metadata != C_NULL
    @assert unsafe_load(dsch.dictionary).metadata == C_NULL
    df2, dd2 = from_c_data(dsp, dap)
    @assert collect(df2.metadata) == ["dk" => "dv"]
    @assert getvalue(df2, dd2, 2) == "hi"
    release!(dd2.buffers[2].region::OwnerRegion)
    _reap!()
    # Canonical (`arrow.`) extension pairs name the dictionary's VALUE type:
    # they ride the value node (on the wrapper they would declare dictionary
    # storage, which PyArrow refuses), and import rejoins them after the
    # wrapper's pairs.
    extmeta = [
        "dk" => "dv",
        "ARROW:extension:name" => "arrow.json",
        "ARROW:extension:metadata" => "",
    ]
    xf = Field(
        "d",
        dvf.type;
        nullable=dvf.nullable,
        metadata=extmeta,
        children=collect(Field, dvf.children),
    )
    xsp, xap = to_c_data(xf, dvd)
    xsch = unsafe_load(xsp)
    @assert Arrow._import_cmetadata(xsch.metadata) == extmeta[1:1]
    @assert Arrow._import_cmetadata(unsafe_load(xsch.dictionary).metadata) == extmeta[2:3]
    xf2, xd2 = from_c_data(xsp, xap)
    @assert collect(xf2.metadata) == extmeta
    release!(xd2.buffers[2].region::OwnerRegion)
    _reap!()
    # Other extension names stay on the wrapper as field metadata, where a
    # consumer that does not know them keeps them.
    appmeta =
        ["ARROW:extension:name" => "dict-extension", "ARROW:extension:metadata" => "m"]
    af = Field(
        "d",
        dvf.type;
        nullable=dvf.nullable,
        metadata=appmeta,
        children=collect(Field, dvf.children),
    )
    asp, aap = to_c_data(af, dvd)
    asch = unsafe_load(asp)
    @assert Arrow._import_cmetadata(asch.metadata) == appmeta
    @assert unsafe_load(asch.dictionary).metadata == C_NULL
    af2, ad2 = from_c_data(asp, aap)
    @assert collect(af2.metadata) == appmeta
    release!(ad2.buffers[2].region::OwnerRegion)
    _reap!()
    # A foreign producer annotating the DEPENDENT node too (extension-type
    # metadata is legal there) loses no pair on import: wrapper pairs
    # first, then the dependent node's.
    wsp, wap = to_c_data(dmf, dvd)
    wsch0 = unsafe_load(wsp)
    vsch0 = unsafe_load(wsch0.dictionary)
    wblob = vcat(
        reinterpret(UInt8, Int32[1]),
        reinterpret(UInt8, Int32[2]),
        codeunits("vk"),
        reinterpret(UInt8, Int32[2]),
        codeunits("vv"),
    )
    GC.@preserve wblob begin
        unsafe_store!(
            wsch0.dictionary,
            CArrowSchema(
                vsch0.format,
                vsch0.name,
                pointer(wblob),
                vsch0.flags,
                vsch0.n_children,
                vsch0.children,
                vsch0.dictionary,
                vsch0.release,
                vsch0.private_data,
            ),
        )
        wf2, wd2 = from_c_data(wsp, wap)
        @assert collect(wf2.metadata) == ["dk" => "dv", "vk" => "vv"]
        release!(wd2.buffers[2].region::OwnerRegion)
        _reap!()
    end
    pf, pd = fromjulia("plain", Int64[1])
    psp, pap = to_c_data(pf, pd)
    pf2, pd2 = from_c_data(psp, pap)
    @assert pf2.metadata === nothing
    release!(pd2.buffers[2].region::OwnerRegion)
    _reap!()
    # Hostile blobs refuse: negative counts and lengths would wrap the walk.
    for negblob in (
        reinterpret(UInt8, Int32[-1]),
        vcat(reinterpret(UInt8, Int32[1]), reinterpret(UInt8, Int32[-5])),
    )
        blob = collect(negblob)
        caught = try
            GC.@preserve blob Arrow._import_cmetadata(pointer(blob))
            false
        catch e
            e isa ValidationError
        end
        @assert caught
    end
    if Sys.iswindows()
        println("C-string POSIX guard-page child skipped on Windows ✓")
    else
        guardscript = joinpath(@__DIR__, "cstring_guard_child.jl")
        guardcmd = `$(Base.julia_cmd()) --startup-file=no --project=$(Base.active_project()) $guardscript`
        success(guardcmd) || error("C-string guard-page child failed")
    end
    println("field metadata crosses the C boundary ✓")
    # Schema-level metadata rides the C stream's struct-typed schema node
    # (the C++/pyarrow convention for `schema.metadata`) in both directions.
    smf, smd = fromjulia("x", Int64[1, 2])
    smsch =
        Schema(Field[smf]; metadata=["schema-k" => "schema-v", "dup" => "a", "dup" => "b"])
    smref = Ref{CArrowArrayStream}()
    GC.@preserve smref begin
        smp = Base.unsafe_convert(Ptr{CArrowArrayStream}, smref)
        export_stream!(smp, smsch, AC.RecordBatch[AC.RecordBatch(smsch, ArrayData[smd], 2)])
        sms = from_c_stream(smp)
        @assert collect(sms.schema.metadata) ==
                ["schema-k" => "schema-v", "dup" => "a", "dup" => "b"]
        smb = nextbatch!(sms)
        @assert smb isa AC.RecordBatch
        @assert collect(smb.schema.metadata) == collect(smsch.metadata)
        @assert nextbatch!(sms) === nothing
        release!(sms)
        release!(smb.columns[1].owner::ForeignOwner)
    end
    _reap!()
    # A schema without metadata imports as `nothing`, not an empty list.
    plainsch = Schema(Field[smf])
    plainref = Ref{CArrowArrayStream}()
    GC.@preserve plainref begin
        plainp = Base.unsafe_convert(Ptr{CArrowArrayStream}, plainref)
        export_stream!(plainp, plainsch, AC.RecordBatch[])
        plains = from_c_stream(plainp)
        @assert plains.schema.metadata === nothing
        @assert nextbatch!(plains) === nothing
        release!(plains)
    end
    _reap!()
    # Metadata is opaque binary data, including schema-only streams.
    binary = String([0xff, 0x00, 0x80])
    for binsch in (
        Schema(Field[smf]; metadata=[binary => "v"]),
        Schema(Field[smf]; metadata=["k" => binary]),
        Schema(Field[Field("x", IntType(64, true); metadata=["k" => binary])]),
    )
        binref = Ref{CArrowArrayStream}()
        GC.@preserve binref begin
            binp = Base.unsafe_convert(Ptr{CArrowArrayStream}, binref)
            export_stream!(binp, binsch, AC.RecordBatch[])
            bins = from_c_stream(binp)
            @assert isequal(bins.schema.metadata, binsch.metadata)
            @assert isequal(bins.schema.fields[1].metadata, binsch.fields[1].metadata)
            @assert nextbatch!(bins) === nothing
            release!(bins)
            @assert binref[].release == C_NULL
        end
        _reap!()
    end
    println("schema metadata crosses the C stream boundary ✓")

    # ---- tables ----------------------------------------------------------

    # The 54-column matrix pyarrow built (test/fixtures-cdata/make_matrix.py):
    # every column crosses both interfaces, in both directions, with the
    # element types and values of an IPC read of the same bytes.
    matrixbytes = read(joinpath(@__DIR__, "fixtures-cdata", "matrix.arrows"))
    tipc = Arrow.Table(matrixbytes)
    @assert length(Tables.columnnames(tipc)) == 54
    function sametable(t)
        ok =
            Tables.columnnames(t) == Tables.columnnames(tipc) &&
            Arrow.getmetadata(t) == Arrow.getmetadata(tipc)
        for (i, nm) in enumerate(Tables.columnnames(tipc))
            a = Tables.getcolumn(tipc, i)
            b = Tables.getcolumn(t, i)
            eltype(a) == eltype(b) && isequal(collect(a), collect(b)) && continue
            ok = false
            println("   MISMATCH ", nm, ": ", eltype(b), " vs ", eltype(a))
        end
        return ok
    end
    # Import alone: the IPC batches go out through the column-level interface
    # and come back as the IPC read of them, whole or one batch at a time. One
    # batch builds every column as the IPC read of that batch does, down to the
    # column type.
    raw = readstream(matrixbytes)
    rawschema = Schema(collect(Field, raw.corefields); metadata=raw.schema.metadata)
    sref = Ref{CArrowArrayStream}()
    GC.@preserve sref begin
        p = Base.unsafe_convert(Ptr{CArrowArrayStream}, sref)
        export_stream!(p, rawschema, raw.batches)
        @assert sametable(Arrow.Table(p))
        export_stream!(p, rawschema, raw.batches)
        @assert [Tables.rowcount(t) for t in Arrow.Stream(p)] == [4, 3]
    end
    sp, ap = to_c_data(_batchfield(rawschema), _structarray(raw.batches[1]))
    tfirst = Arrow.Table(sp, ap)
    ipcfirst = iterate(Arrow.Stream(matrixbytes))[1]
    for (i, nm) in enumerate(Tables.columnnames(ipcfirst))
        a = Tables.getcolumn(ipcfirst, i)
        b = Tables.getcolumn(tfirst, i)
        @assert typeof(a) == typeof(b) && isequal(collect(a), collect(b)) nm
    end
    println("C data and C stream imports read as the IPC read of the same batches ✓")

    sp, ap = to_c_data(tipc)
    @assert unsafe_load(ap).n_children == 54
    @assert sametable(Arrow.Table(sp, ap))
    sref = Ref{CArrowArrayStream}()
    GC.@preserve sref begin
        p = Base.unsafe_convert(Ptr{CArrowArrayStream}, sref)
        export_stream!(p, Arrow.Stream(matrixbytes))        # two batches, two partitions
        @assert sametable(Arrow.Table(p))
        export_stream!(p, Arrow.Stream(matrixbytes))
        parts = collect(Arrow.Stream(p))
        @assert [Tables.rowcount(t) for t in parts] == [4, 3]
        joined = (;
            (
                nm => vcat(
                    collect(Tables.getcolumn(parts[1], i)),
                    collect(Tables.getcolumn(parts[2], i)),
                ) for (i, nm) in enumerate(Tables.columnnames(parts[1]))
            )...
        )
        for (i, nm) in enumerate(Tables.columnnames(tipc))
            @assert isequal(joined[nm], collect(Tables.getcolumn(tipc, i))) nm
            @assert eltype(Tables.getcolumn(parts[1], i)) ==
                    eltype(Tables.getcolumn(tipc, i)) nm
        end
    end
    println("the 54-column matrix crosses both interfaces both ways as an IPC read ✓")

    # An exported table is built as `Arrow.write` builds it: the same fields,
    # column types, and values as the IPC bytes, for plain Julia values too.
    mixed = (
        i=[1, 2, missing],
        f=[1.5, 2.5, 3.5],
        s=["a", missing, "ccc"],
        d=[Dates.Date(2020, 1, 1), Dates.Date(2021, 2, 3), missing],
        dt=[
            Dates.DateTime(2020, 1, 1, 12),
            Dates.DateTime(2021, 2, 3, 4, 5, 6),
            Dates.DateTime(2022, 3, 4),
        ],
        v=[[1, 2], Int[], [3]],
        nt=[(a=1, b="x"), (a=2, b="y"), (a=3, b="z")],
        p=PooledArray(["lo", "hi", "lo"]),
    )
    twrite = Arrow.Table(take!(Arrow.tobuffer(mixed)))
    sp, ap = to_c_data(mixed)
    tc = Arrow.Table(sp, ap)
    for (i, nm) in enumerate(Tables.columnnames(twrite))
        a = Tables.getcolumn(twrite, i)
        b = Tables.getcolumn(tc, i)
        @assert eltype(a) == eltype(b) nm
        @assert isequal(collect(a), collect(b)) nm
        fa = getfield(twrite, :schema).fields[i]
        fb = getfield(tc, :schema).fields[i]
        @assert AC.typeequal(fa.type, fb.type) && fa.nullable == fb.nullable nm
    end
    @assert Tables.columnnames(tc) == Tables.columnnames(twrite)
    # The writer's keywords mean what they mean for `Arrow.write`.
    sp, ap = to_c_data(mixed; dictencode=[:s])
    tk = Arrow.Table(sp, ap)
    @assert getfield(tk, :schema).fields[3].type isa DictionaryType
    @assert getfield(tk, :schema).fields[8].type isa DictionaryType
    @assert isequal(collect(tk.s), ["a", missing, "ccc"])
    sp, ap = to_c_data(mixed; stringview=true)
    tk = Arrow.Table(sp, ap)
    @assert (getfield(tk, :schema).fields[3].type::ViewType).utf8
    @assert isequal(collect(tk.s), ["a", missing, "ccc"])
    @assert try
        to_c_data(mixed; nosuchkeyword=1)
        false
    catch e
        e isa ArgumentError
    end
    @assert try
        to_c_data(mixed; compress=:zstd)
        false
    catch e
        e isa ArgumentError
    end
    println("exported tables match Arrow.write's construction ✓")

    # Exports cross without copying: a Vector column's address and an
    # Arrow.Table view column's address are what the consumer sees.
    xs = collect(Int64, 1:100)
    sp, ap = to_c_data((x=xs,))
    child = unsafe_load(unsafe_load(unsafe_load(ap).children, 1))
    @assert unsafe_load(child.buffers, 2) == Ptr{Cvoid}(pointer(xs))
    @assert isequal(collect(Arrow.Table(sp, ap).x), xs)
    tv = Arrow.Table(take!(Arrow.tobuffer((y=collect(1.0:100.0),))))
    sp, ap = to_c_data(tv)
    child = unsafe_load(unsafe_load(unsafe_load(ap).children, 1))
    source = only(getfield(tv, :regions))
    @assert UInt(source.ptr) <=
            UInt(unsafe_load(child.buffers, 2)) <
            UInt(source.ptr) + source.len
    @assert isequal(collect(Arrow.Table(sp, ap).y), collect(1.0:100.0))
    println("exports cross without copying ✓")

    # A struct array with no rows is a table with no rows; a non-struct array
    # is a one-column table; a table too large for one batch names the stream.
    sp, ap = to_c_data((x=Int64[],))
    @assert Tables.rowcount(Arrow.Table(sp, ap)) == 0
    sp, ap = to_c_data((;))
    @assert Tables.rowcount(Arrow.Table(sp, ap)) == 0
    sp, ap = to_c_data(fromjulia("only", Int64[5, 6])...)
    tone = Arrow.Table(sp, ap)
    @assert Tables.columnnames(tone) == [:only] && collect(tone.only) == [5, 6]
    # An empty first partition only fixes the schema, as for a stream.
    sp, ap = to_c_data(Tables.partitioner([(x=Int64[],), (x=[1, 2],)]))
    @assert collect(Arrow.Table(sp, ap).x) == [1, 2]
    @assert try
        to_c_data(Tables.partitioner([(x=[1],), (x=[2],)]))
        false
    catch e
        e isa ArgumentError && occursin("export_stream!", e.msg)
    end
    sref2 = Ref{CArrowArrayStream}()
    GC.@preserve sref2 begin
        p = Base.unsafe_convert(Ptr{CArrowArrayStream}, sref2)
        export_stream!(p, Tables.partitioner([(x=[1],), (x=[2],)]))
        tp = Arrow.Table(p)
        @assert collect(tp.x) == [1, 2]
        @assert eltype(tp.x) == Union{Missing,Int}        # several partitions: nullable
        export_stream!(p, (x=Int64[],))                    # no rows: no batches
        @assert Tables.rowcount(Arrow.Table(p)) == 0
    end
    println("empty, one-column, and partitioned tables cross ✓")

    # Imported tables read the producer's memory in place: `release!(table)`
    # refuses further column access, and the producer's release runs once
    # the table and its columns are garbage collected.
    key = _released_import_key()
    for _ = 1:10
        _registered(key) || break
        GC.gc(true)
        yield()
        _reap!()
    end
    @assert !_registered(key)
    # Exporting a released table is refused; exporting a table and then
    # releasing it keeps the export valid.
    tr = Arrow.Table(take!(Arrow.tobuffer((x=[1, 2, 3],))))
    sp, ap = to_c_data(tr)
    release!(tr)
    @assert collect(Arrow.Table(sp, ap).x) == [1, 2, 3]
    @assert try
        to_c_data(tr)
        false
    catch e
        e isa InvalidStateException
    end
    println("release! of an imported table refuses access; memory follows the columns ✓")

    # `Arrow.Stream(streamptr)` pulls one batch per iteration and is released
    # like any stream: releasing it, or a table it yielded, stops iteration and
    # column access, and columns already taken stay valid.
    sref3 = Ref{CArrowArrayStream}()
    GC.@preserve sref3 begin
        p = Base.unsafe_convert(Ptr{CArrowArrayStream}, sref3)
        export_stream!(p, Tables.partitioner([(x=[1, 2],), (x=[3],), (x=[4, 5, 6],)]))
        cs = Arrow.Stream(p)
        @assert Tables.columnnames(cs) == [:x]
        @assert Tables.schema(cs).types == (Union{Missing,Int},)
        @assert try
            length(cs)
            false
        catch e
            e isa ArgumentError
        end
        first_table, state = iterate(cs)
        firstx = first_table.x
        @assert collect(firstx) == [1, 2]
        release!(cs)
        @assert try
            iterate(cs, state)
            false
        catch e
            e isa InvalidStateException
        end
        @assert try
            first_table.x
            false
        catch e
            e isa InvalidStateException
        end
        @assert collect(firstx) == [1, 2]
        export_stream!(p, Tables.partitioner([(x=[1, 2],), (x=[3],)]))
        cs = Arrow.Stream(p; stringtype=String)
        first_table, state = iterate(cs)
        release!(first_table)
        @assert try
            iterate(cs, state)
            false
        catch e
            e isa InvalidStateException
        end
        export_stream!(p, Tables.partitioner([(x=[1, 2],), (x=[3],)]))
        @assert [collect(t.x) for t in Arrow.Stream(p; stringtype=String)] == [[1, 2], [3]]
        @assert try
            Arrow.Stream(p; nosuch=1)
            false
        catch e
            e isa ArgumentError && occursin("stringtype, ntasks, and limits", e.msg)
        end
        export_stream!(p, (x=[1, 2],))
        @assert try
            Arrow.Table(p; select=[:x])
            false
        catch e
            e isa ArgumentError && occursin("select", e.msg)
        end
        @assert sref3[].release != C_NULL               # a refused keyword moves nothing
        release!(Arrow.Table(p))
    end
    println("Arrow.Stream over a C stream pulls one batch at a time ✓")

    # `deepcopy` of imported data shares its ownership: after the source is
    # released the copy's reads fail cleanly, and releasing the copy is inert.
    dcf, dcd = fromjulia("deep", Int64[1, 2, 3])
    sp, ap = to_c_data(dcf, dcd)
    dkey = _exportkey(ap)
    dcf2, dcd2 = from_c_data(sp, ap)
    dcopy = deepcopy(dcd2)
    @assert materialize(dcf2, dcopy) == [1, 2, 3]
    release!(dcd2.owner::ForeignOwner)
    @assert try
        materialize(dcf2, dcopy)
        false
    catch e
        e isa InvalidStateException
    end
    release!(dcopy.owner::ForeignOwner)
    _reapreleased!()
    @assert !_registered(dkey)
    println("deepcopy of imported data shares its owner ✓")

    # A vector exported zero-copy and then resized: the consumer keeps reading
    # the bytes it was given, because the export roots the vector's storage,
    # not the vector. Julia 1.10 has no separate storage object to root.
    if VERSION >= v"1.11"
        resized = collect(Int64, 1:1000)
        resizedf, resizedd = fromjulia("resized", resized)
        sp, ap = to_c_data(resizedf, resizedd)
        rkey = _exportkey(ap)
        resizedf2, resizedd2 = from_c_data(sp, ap)
        @assert AC.sliceptr(resizedd2.buffers[2]) == Ptr{UInt8}(pointer(resized))
        storage = WeakRef(AC._storageroot(resized))
        resize!(resized, 1_000_000)
        resized = nothing
        resizedd = nothing
        GC.gc(true)
        @assert storage.value !== nothing
        @assert materialize(Int64, resizedf2, resizedd2) == 1:1000
        release!(resizedd2.owner::ForeignOwner)
        _reapreleased!()
        @assert !_registered(rkey)
        println("resizing an exported vector keeps the consumer's bytes alive ✓")
    end

    childscript = joinpath(@__DIR__, "cdata_stress_child.jl")
    stresscmd = `$(Base.julia_cmd()) --startup-file=no --threads=4 --project=$(Base.active_project()) $childscript`
    success(stresscmd) || error("threaded C Data stress failed")
    println("threaded C Data stress passed in a four-thread child ✓")

    println()
    println("C Data ownership and round-trip checks passed.")
end
