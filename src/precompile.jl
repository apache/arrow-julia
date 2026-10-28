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

# Precompile workload: run the common write/read/scan shapes once during
# package precompilation so first use does not pay their compilation cost.
# Everything here is in-memory and deterministic — no files, no network.
# This file belongs to the top-level Arrow package only; the JuliaC trim gate
# includes src/ArrowCore.jl standalone and must not see PrecompileTools.
import PrecompileTools

PrecompileTools.@setup_workload begin
    columns = (
        ints=Int64[1, 2, 3],
        floats=Float64[1.0, 2.5, 3.5],
        strings=["a", "bb", "ccc"],
        bools=[true, false, true],
        maybe=Union{Missing,Int64}[1, missing, 3],
        maybefloats=Union{Missing,Float64}[1.5, missing, 3.5],
        maybestrings=Union{Missing,String}["a", missing, "ccc"],
        maybesmall=Union{Missing,Int32}[1, missing, 3],
        maybebools=Union{Missing,Bool}[true, missing, false],
        maybedates=[Dates.Date(2020, 1, 1), missing, Dates.Date(2020, 1, 3)],
        lists=[Int64[1, 2], Int64[], Int64[3]],
        dates=[Dates.Date(2020, 1, 1), Dates.Date(2020, 1, 2), Dates.Date(2020, 1, 3)],
        stamps=[
            Dates.DateTime(2020, 1, 1, 12),
            Dates.DateTime(2020, 1, 2, 12),
            Dates.DateTime(2020, 1, 3, 12),
        ],
        pooled=DictEncode(["lo", "hi", "lo"]),
    )
    # pyarrow and Polars declare every field nullable: each flat width both
    # ways, and the common temporal types (timestamps naive and in UTC,
    # durations, dates, times), so a first read of their files finds its
    # column views compiled.
    widths = (
        Int8,
        Int16,
        Int32,
        Int64,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Float16,
        Float32,
        Float64,
        Bool,
    )
    units = (Dates.Second, Dates.Millisecond, Dates.Microsecond, Dates.Nanosecond)
    flat = (;
        (Symbol(:n, nameof(T)) => Union{Missing,T}[one(T), missing] for T in widths)...,
        (Symbol(:p, nameof(T)) => T[one(T), zero(T)] for T in widths)...,
        nstring=Union{Missing,String}["a", missing],
        ndate=Union{Missing,Dates.Date}[Dates.Date(2020, 1, 1), missing],
        ndatetime=Union{Missing,Dates.DateTime}[Dates.DateTime(2020, 1, 1), missing],
        ntime=Union{Missing,Dates.Time}[Dates.Time(12), missing],
        (
            Symbol(:n, nameof(P)) => Union{Missing,Durations.Timestamp{P}}[
                Durations.Timestamp{P}(2020, 1, 1),
                missing,
            ] for P in units
        )...,
        (
            Symbol(:u, nameof(P)) => Union{Missing,Durations.ZonedTimestamp{P,:UTC}}[
                Durations.ZonedTimestamp{P,:UTC}(2020, 1, 1),
                missing,
            ] for P in units
        )...,
        (Symbol(:d, nameof(P)) => Union{Missing,P}[P(1), missing] for P in units)...,
        # Nested columns: their per-batch builders compile once for every row
        # type, so these also leave a first nested read only its row loops.
        nlist=Union{Missing,Vector{Int64}}[[1], missing],
        nfloats=Union{Missing,Vector{Float64}}[[1.0], missing],
        nstrings=Union{Missing,Vector{String}}[["a"], missing],
        nmap=Union{Missing,Dict{String,Int64}}[Dict("a" => 1), missing],
        nstruct=Union{Missing,@NamedTuple{a::Int64,b::String}}[(a=1, b="x"), missing],
        nunion=Union{Int64,String}[1, "a"],
    )
    PrecompileTools.@compile_workload begin
        # The flat table goes out as two record batches, so its columns chain.
        for table in (columns, Tables.partitioner((flat, flat)))
            io = IOBuffer()
            write(io, table)
            flatbytes = take!(seekstart(io))
            tbl = Table(flatbytes)
            for name in Tables.columnnames(tbl)
                column = Tables.getcolumn(tbl, name)
                foreach(identity, column)
                copy(column)
            end
            table === columns && continue
            # A timestamp filter runs on stored integers.
            stamp = Tables.colcmp(>=, Tables.col(:nMicrosecond), Dates.DateTime(2020))
            Table(flatbytes; scan=Tables.Scan(filter=stamp))
        end
        io = IOBuffer()
        write(io, columns)
        bytes = take!(seekstart(io))
        tbl = Table(bytes)
        Tables.columntable(tbl)
        Table(
            bytes;
            scan=Tables.Scan(
                select=(:ints, :floats),
                filter=Tables.col(:ints) > 1,
                limit=2,
            ),
        )
        # Writing what was read: a rewrite, and the read columns as a table,
        # also when each column is read from several batches.
        write(IOBuffer(), tbl)
        write(IOBuffer(), Tables.columntable(tbl))
        io = IOBuffer()
        write(io, columns; batchsize=2)
        write(IOBuffer(), Tables.columntable(Table(take!(seekstart(io)))); batchsize=2)
    end
end
