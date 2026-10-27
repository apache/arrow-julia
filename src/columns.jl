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

# Read-side column builders for flat layouts whose Julia element type has
# the storage layout byte for byte.

"""
One fixed-width column as a Julia vector of `T`, whose layout equals the
Arrow storage of `d`: one bulk copy of the data buffer, then the validity
bitmap's nulls. The element type is `Union{Missing,T}` when `f` is nullable
or the batch holds nulls.
"""
function _bulkfixedcolumn(::Type{T}, f::AC.Field, d::AC.ArrayData, budget=nothing) where {T}
    isbitstype(T) || throw(ArgumentError("a bulk column needs an isbits element type"))
    n = d.len
    w = Int64(sizeof(T))
    src = AC.subslice(
        AC.rolebuffer(d, AC.DATA),
        AC.checked_mul(d.offset, w),
        AC.checked_mul(n, w),
    )
    v = AC.validitybuffer(d)
    # The bitmap is the validity authority; the cached null count may be
    # unset for data that has not passed semantic validation.
    nulls = !AC.isempty_buffer(v) && AC._countsetbits(v, d.offset, n) != n
    if !(f.nullable || nulls)
        _chargevector!(budget, T, n, "fixed-width column")
        out = Vector{T}(undef, n)
        n > 0 && GC.@preserve out d begin
            unsafe_copyto!(Ptr{UInt8}(pointer(out)), AC.sliceptr(src), Int(src.len))
        end
        return out
    end
    _chargevector!(budget, Union{Missing,T}, n, "fixed-width column")
    out = Vector{Union{Missing,T}}(undef, n)
    n > 0 && GC.@preserve d begin
        p = Ptr{T}(AC.sliceptr(src))
        # unsafe_load assumes no alignment, so any buffer offset is valid.
        @inbounds for i = 1:n
            out[i] = unsafe_load(p, i)
        end
    end
    nulls && AC._punchnulls!(out, v, d.offset, n)
    return out
end

"The decimal column of `f` read as stored (a reinterpret: the layouts match)."
_decimalcolumn(f::AC.Field, d::AC.ArrayData, budget=nothing) =
    _bulkfixedcolumn(_decimalhost(f.type::AC.DecimalType), f, d, budget)

# Shared decimals read through `_decimalcolumn`; every other layout keeps the
# typed or dynamic path.
_isbulkdecimal(f::AC.Field) = f.type isa AC.DecimalType && _shareddecimal(f.type)
