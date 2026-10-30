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

# An ordered CategoricalArray writes an ordered Arrow dictionary, and
# `isordered` reads a dictionary column's flag.
module ArrowCategoricalArraysExt

import Arrow
import CategoricalArrays

Arrow._isordered(x::CategoricalArrays.CategoricalArray) = CategoricalArrays.isordered(x)
CategoricalArrays.isordered(x::Arrow.DictEncoded) = Arrow._isordered(x)
CategoricalArrays.isordered(x::Arrow.ChainedVector{T,<:Arrow.DictEncoded}) where {T} =
    Arrow._isordered(x)

end
