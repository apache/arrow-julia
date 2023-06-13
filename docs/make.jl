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

using Documenter
using Arrow


makedocs(;
    modules=[Arrow],
    repo="https://github.com/apache/arrow-julia/blob/{commit}{path}#L{line}",
    sitename="Arrow.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://arrow.juliadata.org/",
        assets=String[],
    ),
    pages = [
        "Home" => "index.md",
        "User Manual" => "manual.md",
        "API Reference" => "reference.md"
    ]
)

deploydocs(;
    repo="github.com/apache/arrow-julia",
    devbranch = "main",
    branch = "asf-site"
)
