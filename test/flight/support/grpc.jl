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

function with_test_grpc_handle(f::F) where {F}
    grpc = gRPCClient.gRPCCURL()
    gRPCClient.grpc_init(grpc)
    try
        return f(grpc)
    finally
        gRPCClient.grpc_shutdown(grpc)
    end
end

function load_grpcserver()
    isnothing(Base.find_package("gRPCServer")) && return nothing
    return Base.require(
        Base.PkgId(Base.UUID("608c6337-0d7d-447f-bb69-0f5674ee3959"), "gRPCServer"),
    )
end
