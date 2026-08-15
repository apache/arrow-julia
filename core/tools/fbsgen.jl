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

# =============================================================================
# fbsgen.jl — regenerate the FlatBuffers metadata bindings from the Arrow
# format's .fbs files, in the exact idiom of the vendored hand-written
# bindings (src/metadata/*.jl) over the vendored src/FlatBuffers runtime.
#
#     julia core/tools/fbsgen.jl <fbs-dir> <out-dir>
#
# The vendored bindings were hand-written against a 2020-era schema and have
# accumulated eight known drifts from the current spec (variadicBufferCounts
# typed as Int32 instead of [long]; the Type tag table stopping at 21;
# Schema.features missing; IntervalUnit lacking MONTH_DAY_NANO; Decimal
# bitWidth default; RecordBatch four slots; DictionaryKind missing; the
# `largUtf8Start` typo). Hand-patching those is exactly the bug class that
# produced them. This tool makes regeneration mechanical: parse the schema,
# emit bindings, diff. Rerun whenever format/*.fbs moves.
#
# Scope: the subset of the FlatBuffers IDL that Arrow's three schemas use —
# `table`, `struct`, `enum` (with explicit values), `union`, scalar and
# vector fields, table/string references, defaults, `(deprecated)`, and
# `namespace`/`root_type` (ignored). Comments become nothing; the .fbs is the
# documentation. Julia name collisions with Base/Core (`Int`, `Bool`, `Type`,
# `Struct_`) are resolved exactly as the hand-written bindings resolved them
# so existing user code (`Meta.Int`, `Meta.Bool`, `Meta.Struct`) keeps working.
# =============================================================================

module FbsGen

struct FbsField
    name::String
    type::String          # raw IDL type: "int", "[Buffer]", "string", "Type", ...
    default::Union{Nothing,String}
    deprecated::Bool
end

struct FbsTable
    name::String
    isstruct::Bool
    fields::Vector{FbsField}
end

struct FbsEnum
    name::String
    basetype::String
    members::Vector{Pair{String,Int}}
    isunion::Bool
end

# --- tokenizer / parser ---------------------------------------------------------

function _strip_comments(src::String)
    out = IOBuffer()
    i = 1
    n = ncodeunits(src)
    while i <= n
        c = src[i]
        if c == '/' && i < n && src[i + 1] == '/'
            j = findnext('\n', src, i)
            i = j === nothing ? n + 1 : j
        elseif c == '/' && i < n && src[i + 1] == '*'
            j = findnext("*/", src, i)
            i = j === nothing ? n + 1 : last(j) + 1
        else
            print(out, c)
            i = nextind(src, i)
        end
    end
    return String(take!(out))
end

_ident(s) = strip(s)

"""
Parse one .fbs source into ordered declarations. Order is preserved: the
emitted Julia must define types before their users, and .fbs authors already
order dependencies for flatc.
"""
function parsefbs(src::String)
    src = _strip_comments(src)
    decls = Any[]
    # Attributes/includes/namespace/root_type lines
    src = replace(src, r"\binclude\s+\"[^\"]*\";" => "")
    src = replace(src, r"\bnamespace\s+[\w.]+;" => "")
    src = replace(src, r"\broot_type\s+\w+;" => "")
    pos = 1
    while true
        m = match(r"\b(table|struct|enum|union)\s+(\w+)\s*(?::\s*(\w+))?\s*\{", src, pos)
        m === nothing && break
        kind, name, base = m.captures
        bodystart = m.offset + ncodeunits(m.match)
        depth = 1
        i = bodystart
        while depth > 0
            i = nextind(src, i - 1) + 0
            i > ncodeunits(src) && error("unterminated $kind $name")
            c = src[i]
            c == '{' && (depth += 1)
            c == '}' && (depth -= 1)
            i += 1
        end
        body = src[bodystart:(i - 2)]
        pos = i
        if kind == "table" || kind == "struct"
            fields = FbsField[]
            for stmt in split(body, ';')
                s = strip(stmt)
                isempty(s) && continue
                s = replace(s, r"\[\s+" => "[", r"\s+\]" => "]")   # `[ int ]` -> `[int]`
                fm = match(r"^(\w+)\s*:\s*(\[?[\w.]+\]?)\s*(?:=\s*([^\s(]+))?\s*(\([^)]*\))?$", s)
                fm === nothing && error("cannot parse field '$s' in $name")
                fname, ftype, fdefault, attrs = fm.captures
                # Message.fbs qualifies cross-file types
                # (`org.apache.arrow.flatbuf.MetadataVersion`); all three
                # schemas share one Julia module, so keep the leaf name.
                ftype = replace(ftype, r"[\w.]*\.(\w+)" => s"\1")
                push!(fields, FbsField(fname, ftype, fdefault,
                    attrs !== nothing && occursin("deprecated", attrs)))
            end
            push!(decls, FbsTable(name, kind == "struct", fields))
        else
            members = Pair{String,Int}[]
            # FlatBuffers unions reserve tag 0 for the implicit NONE member,
            # so the first NAMED union member is 1; enums start at 0.
            next = kind == "union" ? 1 : 0
            for stmt in split(body, ',')
                s = strip(stmt)
                isempty(s) && continue
                em = match(r"^(\w+)\s*(?:=\s*(-?\d+))?$", s)
                em === nothing && error("cannot parse enum member '$s' in $name")
                v = em.captures[2] === nothing ? next : parse(Int, em.captures[2])
                push!(members, em.captures[1] => v)
                next = v + 1
            end
            push!(decls, FbsEnum(name, something(base, kind == "union" ? "ubyte" : "int"),
                members, kind == "union"))
        end
    end
    return decls
end

# --- type mapping -----------------------------------------------------------------

const SCALARS = Dict(
    "bool" => ("Base.Bool", 1), "byte" => ("Int8", 1), "ubyte" => ("UInt8", 1),
    "short" => ("Int16", 2), "ushort" => ("UInt16", 2), "int" => ("Int32", 4),
    "uint" => ("UInt32", 4), "long" => ("Int64", 8), "ulong" => ("UInt64", 8),
    "float" => ("Float32", 4), "double" => ("Float64", 8),
    "int8" => ("Int8", 1), "uint8" => ("UInt8", 1), "int16" => ("Int16", 2),
    "uint16" => ("UInt16", 2), "int32" => ("Int32", 4), "uint32" => ("UInt32", 4),
    "int64" => ("Int64", 8), "uint64" => ("UInt64", 8),
    "float32" => ("Float32", 4), "float64" => ("Float64", 8))

# The hand-written bindings' name choices, kept for source compatibility.
const RENAMES = Dict("Struct_" => "Struct")
jlname(n::AbstractString) = get(RENAMES, String(n), String(n))

isvector(t) = startswith(t, "[")
elemtype(t) = t[2:(end - 1)]

# --- emitter ---------------------------------------------------------------------

lowerfirst(s) = isempty(s) ? s : lowercase(s[1:1]) * s[2:end]
# Some builder names in the hand-written files strip underscores/camelCase
# differently; we normalize to lowerFirst(TableName) + CamelCase(field), which
# matches every name the prove-out actually calls (verified by the rewire).
camel(s) = join(uppercasefirst.(split(s, '_')))

function emit(decls, io::IO; alldecls=decls)
    # Name resolution spans every generated schema (Message.fbs references
    # Schema.fbs tables; all three land in one module), so `alldecls`
    # supplies the known-name set while `decls` drives emission order.
    enums = Dict{String,FbsEnum}()
    tables = Dict{String,FbsTable}()
    for d in alldecls
        d isa FbsEnum && (enums[d.name] = d)
        d isa FbsTable && (tables[d.name] = d)
    end
    for d in decls
        if d isa FbsEnum && !d.isunion
            base = SCALARS[d.basetype][1]
            print(io, "@enumx ", d.name, "::", base, " ")
            println(io, join(("$(m.first)=$(m.second)" for m in d.members), " "))
            println(io)
        elseif d isa FbsEnum && d.isunion
            # Tag -> type and type -> tag ladders, matching the hand-written
            # `Type(b::UInt8)`/`Type(::Base.Type{T})` and `MessageHeader` shape.
            # A bare `function X end` first creates a MODULE-LOCAL generic, so
            # a union named `Type` shadows `Base.Type` instead of extending it.
            println(io, "function ", d.name, " end")
            println(io)
            # Members whose tables come from schemas we do not generate
            # (Tensor/SparseTensor live in Tensor.fbs) are emitted as comments
            # — the same choice the hand-written bindings made — so the ladder
            # neither references undefined names nor silently drops the tag.
            known(m) = haskey(tables, m) || haskey(enums, m)
            println(io, "function ", d.name, "(b::UInt8)")
            for (mname, v) in d.members
                mname == "NONE" && continue
                pre = known(mname) ? "    " : "    # "
                println(io, pre, "b == ", v, " && return ", jlname(mname))
            end
            println(io, "    return nothing")
            println(io, "end")
            println(io)
            println(io, "function ", d.name, "(::Base.Type{T})::Int16 where {T}")
            for (mname, v) in d.members
                mname == "NONE" && continue
                pre = known(mname) ? "    " : "    # "
                println(io, pre, "T == ", jlname(mname), " && return ", v)
            end
            println(io, "    return 0")
            println(io, "end")
            println(io)
        elseif d isa FbsTable && d.isstruct
            emitstruct(io, d)
        elseif d isa FbsTable
            emittable(io, d, enums, tables)
        end
    end
end

function emitstruct(io::IO, d::FbsTable)
    name = jlname(d.name)
    println(io, "struct ", name, " <: FlatBuffers.Struct")
    println(io, "    bytes::Vector{UInt8}")
    println(io, "    pos::Base.Int")
    println(io, "end")
    println(io)
    # Layout: natural alignment of each scalar, total padded to max alignment.
    off = 0
    maxalign = 1
    layout = Tuple{String,String,Int}[]
    for f in d.fields
        jt, sz = SCALARS[f.type]
        off = cld(off, sz) * sz
        push!(layout, (f.name, jt, off))
        off += sz
        maxalign = max(maxalign, sz)
    end
    total = cld(off, maxalign) * maxalign
    println(io, "FlatBuffers.structsizeof(::Base.Type{", name, "}) = ", total)
    println(io)
    println(io, "Base.propertynames(x::", name, ") = (",
        join((":" * f for (f, _, _) in layout), ", "), length(layout) == 1 ? ",)" : ")")
    println(io)
    println(io, "function Base.getproperty(x::", name, ", field::Symbol)")
    firstbranch = true
    for (f, jt, o) in layout
        println(io, "    ", firstbranch ? "if" : "elseif", " field === :", f)
        println(io, "        return FlatBuffers.get(x, FlatBuffers.pos(x)",
            o == 0 ? "" : " + $o", ", ", jt, ")")
        firstbranch = false
    end
    println(io, "    end")
    println(io, "    return nothing")
    println(io, "end")
    println(io)
    args = join(("$(f)::$(jt)" for (f, jt, _) in layout), ", ")
    println(io, "function create", name, "(b::FlatBuffers.Builder, ", args, ")")
    println(io, "    FlatBuffers.prep!(b, ", maxalign, ", ", total, ")")
    # prepend in reverse, inserting pad where the layout has gaps
    prevoff = total
    for (f, jt, o) in reverse(layout)
        sz = SCALARS[first(k for (k, v) in SCALARS if v[1] == jt)][2]
        pad = prevoff - (o + sz)
        pad > 0 && println(io, "    FlatBuffers.pad!(b, ", pad, ")")
        println(io, "    prepend!(b, ", f, ")")
        prevoff = o
    end
    println(io, "    return FlatBuffers.offset(b)")
    println(io, "end")
    println(io)
end

function emittable(io::IO, d::FbsTable, enums, tables)
    name = jlname(d.name)
    lname = lowerfirst(name)
    println(io, "struct ", name, " <: FlatBuffers.Table")
    println(io, "    bytes::Vector{UInt8}")
    println(io, "    pos::Base.Int")
    println(io, "end")
    println(io)
    # Union fields occupy TWO vtable slots (type tag, value); count slots.
    slots = String[]
    slotof = Dict{String,Int}()
    for f in d.fields
        if haskey(enums, f.type) && enums[f.type].isunion
            slotof[f.name * "_type"] = length(slots); push!(slots, f.name * "_type")
        end
        slotof[f.name] = length(slots); push!(slots, f.name)
    end
    props = [f.name for f in d.fields if !f.deprecated]
    println(io, "Base.propertynames(x::", name, ") = (",
        join((":" * p for p in props), ", "), length(props) == 1 ? ",)" : ")")
    println(io)
    if !isempty(props)
        println(io, "function Base.getproperty(x::", name, ", field::Symbol)")
        firstbranch = true
        for f in d.fields
            f.deprecated && continue
            vo = 4 + 2 * slotof[f.name]
            println(io, "    ", firstbranch ? "if" : "elseif", " field === :", f.name)
            firstbranch = false
            t = f.type
            if haskey(enums, t) && enums[t].isunion
                # tag slot precedes value slot
                tvo = 4 + 2 * slotof[f.name * "_type"]
                println(io, "        o = FlatBuffers.offset(x, ", tvo, ")")
                println(io, "        if o != 0")
                println(io, "            T = ", t, "(FlatBuffers.get(x, o + FlatBuffers.pos(x), UInt8))")
                println(io, "            o = FlatBuffers.offset(x, ", vo, ")")
                println(io, "            pos = FlatBuffers.union(x, o)")
                println(io, "            if o != 0")
                println(io, "                return FlatBuffers.init(T, FlatBuffers.bytes(x), pos)")
                println(io, "            end")
                println(io, "        end")
            elseif haskey(enums, t)
                e = enums[t]
                println(io, "        o = FlatBuffers.offset(x, ", vo, ")")
                println(io, "        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), ", t, ".T)")
                # default: explicit or first member
                dv = f.default === nothing ? e.members[1].first : f.default
                println(io, "        return ", t, ".", dv)
            elseif haskey(SCALARS, t)
                jt = SCALARS[t][1]
                println(io, "        o = FlatBuffers.offset(x, ", vo, ")")
                println(io, "        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), ", jt, ")")
                if f.default !== nothing
                    dv = f.default
                    println(io, "        return ", jt == "Base.Bool" ? dv : "$jt($dv)")
                elseif jt == "Base.Bool"
                    println(io, "        return false")
                else
                    println(io, "        return ", jt, "(0)")
                end
            elseif t == "string"
                println(io, "        o = FlatBuffers.offset(x, ", vo, ")")
                println(io, "        o != 0 && return String(x, o + FlatBuffers.pos(x))")
            elseif isvector(t)
                et = elemtype(t)
                jt = haskey(SCALARS, et) ? SCALARS[et][1] :
                    haskey(enums, et) ? et * ".T" : jlname(et)
                println(io, "        o = FlatBuffers.offset(x, ", vo, ")")
                println(io, "        if o != 0")
                println(io, "            return FlatBuffers.Array{", jt, "}(x, o)")
                println(io, "        end")
            else # table reference
                println(io, "        o = FlatBuffers.offset(x, ", vo, ")")
                println(io, "        if o != 0")
                println(io, "            y = FlatBuffers.indirect(x, o + FlatBuffers.pos(x))")
                println(io, "            return FlatBuffers.init(", jlname(t), ", FlatBuffers.bytes(x), y)")
                println(io, "        end")
            end
        end
        println(io, "    end")
        println(io, "    return nothing")
        println(io, "end")
        println(io)
    end
    # builders
    println(io, lname, "Start(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, ", length(slots), ")")
    for f in d.fields
        f.deprecated && continue
        t = f.type
        fc = camel(f.name)
        slot = slotof[f.name]
        if haskey(enums, t) && enums[t].isunion
            tslot = slotof[f.name * "_type"]
            println(io, lname, "Add", fc, "Type(b::FlatBuffers.Builder, ::Core.Type{T}) where {T} =")
            println(io, "    FlatBuffers.prependslot!(b, ", tslot, ", ", t, "(T), 0)")
            println(io, lname, "Add", fc, "(b::FlatBuffers.Builder, ", f.name, "::FlatBuffers.UOffsetT) =")
            println(io, "    FlatBuffers.prependoffsetslot!(b, ", slot, ", ", f.name, ", 0)")
        elseif haskey(enums, t)
            e = enums[t]
            # The runtime compares `x != T(default)`, so the default is the
            # member's INTEGER value (constructible into the enum), never
            # the enum instance itself.
            dname = f.default === nothing ? e.members[1].first : f.default
            dval = something(findfirst(m -> m.first == dname, e.members), 1)
            println(io, lname, "Add", fc, "(b::FlatBuffers.Builder, ", f.name, "::", t, ".T) =")
            println(io, "    FlatBuffers.prependslot!(b, ", slot, ", ", f.name, ", ", e.members[dval].second, ")")
        elseif haskey(SCALARS, t)
            jt = SCALARS[t][1]
            dv = f.default === nothing ? (jt == "Base.Bool" ? "false" : "0") : f.default
            println(io, lname, "Add", fc, "(b::FlatBuffers.Builder, ", f.name, "::", jt, ") =")
            println(io, "    FlatBuffers.prependslot!(b, ", slot, ", ", f.name, ", ", dv, ")")
        else # string / vector / table: offset slot
            println(io, lname, "Add", fc, "(b::FlatBuffers.Builder, ", f.name, "::FlatBuffers.UOffsetT) =")
            println(io, "    FlatBuffers.prependoffsetslot!(b, ", slot, ", ", f.name, ", 0)")
            if isvector(t)
                et = elemtype(t)
                esz, ealign = if haskey(SCALARS, et)
                    (SCALARS[et][2], SCALARS[et][2])
                elseif haskey(enums, et)
                    (SCALARS[enums[et].basetype][2], SCALARS[enums[et].basetype][2])
                elseif haskey(tables, et) && tables[et].isstruct
                    st = tables[et]
                    szs = [SCALARS[ff.type][2] for ff in st.fields]
                    off = 0; ma = 1
                    for s in szs; off = cld(off, s) * s + s; ma = max(ma, s); end
                    (cld(off, ma) * ma, ma)
                else
                    (4, 4)
                end
                println(io, lname, "Start", fc, "Vector(b::FlatBuffers.Builder, numelems) =")
                println(io, "    FlatBuffers.startvector!(b, ", esz, ", numelems, ", ealign, ")")
            end
        end
    end
    println(io, lname, "End(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)")
    println(io)
end

const HEADER = """
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

# GENERATED by core/tools/fbsgen.jl from apache/arrow format/{name}.fbs —
# do not edit by hand; rerun the generator against the current spec.

"""

function generate(fbsdir::AbstractString, outdir::AbstractString)
    mkpath(outdir)
    names = ("Schema", "File", "Message")
    parsed = Dict(n => parsefbs(read(joinpath(fbsdir, n * ".fbs"), String)) for n in names)
    alldecls = reduce(vcat, (parsed[n] for n in names))
    for name in names
        decls = parsed[name]
        io = IOBuffer()
        print(io, replace(HEADER, "{name}" => name))
        emit(decls, io; alldecls=alldecls)
        write(joinpath(outdir, name * ".jl"), take!(io))
        println("generated ", name, ".jl: ", length(decls), " declarations")
    end
    write(joinpath(outdir, "Flatbuf.jl"), replace(HEADER, "{name}" => "*") * """
module Flatbuf

using EnumX
using ..FlatBuffers

include("Schema.jl")
include("File.jl")
include("Message.jl")

end # module
""")
    return nothing
end

end # module FbsGen

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
    length(ARGS) == 2 || error("usage: julia fbsgen.jl <fbs-dir> <out-dir>")
    FbsGen.generate(ARGS[1], ARGS[2])
end
