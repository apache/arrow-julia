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

# Local paths share the string-path source and atomic destination behavior.
module ArrowFilePathsBaseExt

import Arrow
using FilePathsBase: SystemPath

Arrow.Table(path::SystemPath; kwargs...) = Arrow.Table(string(path); kwargs...)
Arrow.Stream(path::SystemPath; kwargs...) = Arrow.Stream(string(path); kwargs...)
Arrow._opensink(path::SystemPath) = Arrow._openpath(string(path))

function Arrow.append(path::SystemPath, table; kwargs...)
    Arrow.append(string(path), table; kwargs...)
    return path
end
Arrow.append(path::SystemPath; kwargs...) = table -> Arrow.append(path, table; kwargs...)

end
