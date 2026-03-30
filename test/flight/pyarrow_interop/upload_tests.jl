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

function pyarrow_interop_test_upload(client, upload_descriptor)
    upload_source = Tables.partitioner((
        (id=Int64[10, 11], name=["ten", "eleven"]),
        (id=Int64[12], name=["twelve"]),
    ))
    upload_messages = Arrow.Flight.flightdata(upload_source; descriptor=upload_descriptor)

    doput_req, doput_request, doput_response = Arrow.Flight.doput(client)
    put_results = pyarrow_interop_send_messages(
        doput_req,
        doput_request,
        doput_response,
        upload_messages,
    )

    @test !isempty(put_results)
    @test String(put_results[end].app_metadata) == "stored"

    uploaded_info = Arrow.Flight.getflightinfo(client, upload_descriptor)
    uploaded_req, uploaded_channel =
        Arrow.Flight.doget(client, uploaded_info.endpoint[1].ticket)
    uploaded_messages = pyarrow_interop_collect(uploaded_req, uploaded_channel)

    uploaded_table = Arrow.Flight.table(uploaded_messages; schema=uploaded_info)
    @test uploaded_table.id == [10, 11, 12]
    @test uploaded_table.name == ["ten", "eleven", "twelve"]
end
