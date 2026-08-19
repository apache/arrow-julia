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

# Alias every Arrow and ArrowCore binding into the including module so the
# batteries read as they were written: unqualified names. `names(all=true)`
# covers Arrow's own bindings; ArrowCore's exported names reach Arrow through
# `using` and need listing explicitly. Skipped: the module's own eval/include,
# and the facade names the batteries never use unqualified.
for n in union(names(Arrow; all=true), names(Arrow.ArrowCore))
    sn = String(n)
    (startswith(sn, "#") || n in (:eval, :include, :Arrow, :write, :Table, :Stream)) &&
        continue
    isdefined(Arrow, n) || continue
    @eval const $n = Arrow.$n
end
