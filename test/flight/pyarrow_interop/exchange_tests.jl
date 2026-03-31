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

function pyarrow_interop_test_exchange(client, exchange_descriptor)
    exchange_source = Tables.partitioner((
        (id=Int64[21, 22], name=["twenty-one", "twenty-two"]),
        (id=Int64[23], name=["twenty-three"]),
    ))
    exchange_messages =
        Arrow.Flight.flightdata(exchange_source; descriptor=exchange_descriptor)

    exchange_req, exchange_request, exchange_response = Arrow.Flight.doexchange(client)
    exchanged_messages = pyarrow_interop_send_messages(
        exchange_req,
        exchange_request,
        exchange_response,
        exchange_messages,
    )

    exchange_table = Arrow.Flight.table(exchanged_messages)
    @test exchange_table.id == [21, 22, 23]
    @test exchange_table.name == ["twenty-one", "twenty-two", "twenty-three"]
    @test filter(!isempty, getfield.(exchanged_messages, :app_metadata)) ==
          [b"exchange:0", b"exchange:1"]
end
