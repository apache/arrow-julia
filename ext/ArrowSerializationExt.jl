# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

module ArrowSerializationExt

import Arrow
import Serialization
const AC = Arrow.ArrowCore
const S = Serialization

# Write only the physical values. ForeignOwner, OwnerRegion, ReleaseCell,
# raw addresses, and semantic certificates must never cross this boundary.
function S.serialize(s::S.AbstractSerializer, d::AC.ArrayData)
    S.serialize_cycle_header(s, d) && return nothing
    S.serialize(s, d.type)
    S.serialize(s, d.len)
    S.serialize(s, d.offset)
    S.serialize(s, [AC.slicebytes(b) for b in d.buffers])
    S.serialize(s, d.children)
    S.serialize(s, d.dictionary)
    S.serialize(s, @atomic(:monotonic, d.nullcount))
    return nothing
end

function S.deserialize(s::S.AbstractSerializer, ::Type{AC.ArrayData})
    type = S.deserialize(s)
    len = S.deserialize(s)
    offset = S.deserialize(s)
    buffers = S.deserialize(s)
    children = S.deserialize(s)
    dictionary = S.deserialize(s)
    nullcount = S.deserialize(s)
    d = AC.ArrayData(
        type,
        len,
        [AC._databuffer(b) for b in buffers];
        offset=offset,
        children=children,
        dictionary=dictionary,
        nullcount=nullcount,
    )
    # ArrayData's immutable topology cannot contain a cycle. Register after
    # its children are read to preserve repeated references to the same node.
    S.deserialize_cycle(s, d)
    return d
end

function S.serialize(s::S.AbstractSerializer, t::Arrow.Table)
    S.serialize_type(s, Arrow.Table)
    for name in (:names, :columns, :schema, :nrows, :retainedpools)
        S.serialize(s, getfield(t, name))
    end
    return nothing
end

function S.deserialize(s::S.AbstractSerializer, ::Type{Arrow.Table})
    names = S.deserialize(s)
    columns = S.deserialize(s)
    schema = S.deserialize(s)
    nrows = S.deserialize(s)
    pools = S.deserialize(s)
    return Arrow._table(names, columns, schema, AC.OwnerRegion[], nrows, pools)
end

end # module ArrowSerializationExt
