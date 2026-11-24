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

# FileIO.jl registers `.arrow` files (magic `ARROW1`) as `format"Arrow"` and
# hands them to this package: `load(path)` reads an `Arrow.Table` and
# `save(path, table)` writes any Tables.jl table, with `Arrow.Table`'s and
# `Arrow.write`'s keywords.
module ArrowFileIOExt

import Arrow
using FileIO: File, Stream, @format_str, filename, stream

Arrow.fileio_load(f::File{format"Arrow"}; kwargs...) = Arrow.Table(filename(f); kwargs...)
Arrow.fileio_load(s::Stream{format"Arrow"}; kwargs...) = Arrow.Table(stream(s); kwargs...)

function Arrow.fileio_save(f::File{format"Arrow"}, table; kwargs...)
    Arrow.write(filename(f), table; kwargs...)
    return nothing
end
function Arrow.fileio_save(s::Stream{format"Arrow"}, table; kwargs...)
    Arrow.write(stream(s), table; kwargs...)
    return nothing
end

end # module ArrowFileIOExt
