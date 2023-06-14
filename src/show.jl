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

# 2-arg show: show schema and list # of metadata entries if non-zero
function Base.show(io::IO, table::Table)
    ncols = length(Tables.columnnames(table))
    print(io, "$(typeof(table)) with $(Tables.rowcount(table)) rows, $(ncols) columns,")
    meta = getmetadata(table)
    if meta !== nothing && !isempty(meta)
        print(io, " ", length(meta), " metadata entries,")
    end
    sch = Tables.schema(table)
    print(io, " and schema:\n")
    show(IOContext(io, :print_schema_header => false), sch)
    return nothing
end

# 3-arg show: show schema and show metadata entries adaptively according to `displaysize`
function Base.show(io::IO, mime::MIME"text/plain", table::Table)
    display_rows, display_cols = displaysize(io)
    ncols = length(Tables.columnnames(table))
    meta = getmetadata(table)
    if meta !== nothing
        display_rows -= 1 # decrement for metadata header line
        display_rows -= min(length(meta), 2) # decrement so we can show at least 2 lines of metadata
    end
    print(
        io,
        "$(typeof(table)) with $(Tables.rowcount(table)) rows, $(ncols) columns, and ",
    )
    sch = Tables.schema(table)
    print(io, "schema:\n")
    schema_context = IOContext(
        io,
        :print_schema_header => false,
        :displaysize => (max(display_rows, 3), display_cols),
    )
    schema_str = sprint(show, mime, sch; context=schema_context)
    print(io, schema_str)
    display_rows -= (count("\n", schema_str) + 1) # decrement for number of lines printed
    if meta !== nothing
        print(io, "\n\nwith metadata given by a ")
        show(
            IOContext(io, :displaysize => (max(display_rows, 5), display_cols)),
            mime,
            meta,
        )
    end
    return nothing
end
