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

function pyarrow_interop_test_discovery(client, protocol, download_descriptor)
    flights_req, flights_channel = Arrow.Flight.listflights(client)
    flights = pyarrow_interop_collect(flights_req, flights_channel)
    @test any(
        info ->
            !isnothing(info.flight_descriptor) &&
            info.flight_descriptor.path == download_descriptor.path,
        flights,
    )

    actions_req, actions_channel = Arrow.Flight.listactions(client)
    actions = pyarrow_interop_collect(actions_req, actions_channel)
    @test any(action -> action.var"#type" == "ping", actions)

    action_req, action_channel =
        Arrow.Flight.doaction(client, protocol.Action("ping", UInt8[]))
    action_results = pyarrow_interop_collect(action_req, action_channel)
    @test length(action_results) == 1
    @test String(action_results[1].body) == "pong"
end
