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
    repo=Remotes.GitHub("apache", "arrow-julia"),
    sitename="Arrow.jl",
    # The reference page documents the public surface explicitly; internal
    # helpers carry docstrings for maintainers and are not part of the site.
    checkdocs=:public,
    checkdocs_ignored_modules=[Arrow.ArrowCore, Arrow.FlatBuffers, Arrow.Meta],
    format=Documenter.HTML(;
        prettyurls=true,
        canonical="https://arrow.apache.org/julia/",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
        "User Manual" => "manual.md",
        "Migrating from 2.x" => "migration.md",
        "API Reference" => "reference.md",
    ],
    pagesonly=true,
)

deploydocs(; repo="github.com/apache/arrow-julia", devbranch="main", branch="asf-site")
