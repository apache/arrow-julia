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

# Delete `path`, which views that are now garbage may still map. Windows
# refuses to delete a mapped file until the mapping's finalizer has run, so
# this collects and retries, and in the end leaves the file in the temporary
# directory: deleting it is cleanup, not a check.
function rmmapped(path::AbstractString)
    for _ = 1:20
        GC.gc(true)
        try
            rm(path; force=true)
            return true
        catch err
            (Sys.iswindows() && err isa Base.IOError) || rethrow()
            sleep(0.05)
        end
    end
    return false
end
