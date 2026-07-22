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

module ArrowTypesUUIDsExt

import ArrowTypes
import UUIDs: UUID

ArrowTypes.ArrowKind(::Type{UUID}) = ArrowTypes.FixedSizeListKind{16,UInt8}()
ArrowTypes.ArrowType(::Type{UUID}) = NTuple{16,UInt8}
ArrowTypes.toarrow(x::UUID) = reinterpret(NTuple{16,UInt8}, x.value)
const UUID_SYMBOL = Symbol("JuliaLang.UUID")
ArrowTypes.arrowname(::Type{UUID}) = UUID_SYMBOL
ArrowTypes.JuliaType(::Val{UUID_SYMBOL}) = UUID
ArrowTypes.fromarrow(::Type{UUID}, x::NTuple{16,UInt8}) = UUID(reinterpret(UInt128, x))

end # module ArrowTypesUUIDsExt
