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

function pyarrow_interop_test_download(client, download_descriptor)
    download_info = Arrow.Flight.getflightinfo(client, download_descriptor)
    @test download_info.total_records == 3
    @test length(download_info.endpoint) == 1

    download_schema = Arrow.Flight.getschema(client, download_descriptor)
    @test Arrow.Flight.schemaipc(download_schema) == Arrow.Flight.schemaipc(download_info)

    doget_req, doget_channel = Arrow.Flight.doget(client, download_info.endpoint[1].ticket)
    download_messages = pyarrow_interop_collect(doget_req, doget_channel)

    download_table = Arrow.Flight.table(download_messages; schema=download_info)
    @test download_table.id == [1, 2, 3]
    @test download_table.name == ["one", "two", "three"]
end
