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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

module AcceptanceSupport

import Arrow
using PooledArrays
using Tables
import Base64

const AC = Arrow.AC

# The acceptance implementation deliberately exercises private adapter and
# Core seams. Keep that dependency explicit: adding or removing one of these
# names is a reviewable test-interface change, not an automatic consequence of
# an unrelated package binding.
import Arrow:
    ARROW_FLAG_DICTIONARY_ORDERED,
    ARROW_FLAG_MAP_KEYS_SORTED,
    AllocationBudget,
    AllocationLimitError,
    ArrayData,
    ArrowFile,
    ArrowType,
    BinaryType,
    BoolType,
    BufferSlice,
    CArrowArray,
    CArrowArrayStream,
    CArrowSchema,
    CODEC_LZ4_FRAME,
    CODEC_ZSTD,
    CONTINUATION,
    DateType,
    DecimalType,
    DecodeCursor,
    DecodeState,
    DictionaryType,
    DurationType,
    EINVAL,
    EXPERIMENTAL_COMPRESSION_KEY,
    EXPORT_REGISTRY,
    FB,
    FILE_MAGIC,
    Field,
    FixedSizeBinaryType,
    FixedSizeListType,
    ForeignOwner,
    IPCStream,
    IntType,
    IntervalType,
    Limits,
    ListType,
    ListViewType,
    MapType,
    Meta,
    NEXT_KEY,
    NullType,
    OwnerRegion,
    REGISTRY_LOCK,
    RecordBatch,
    ReleaseCounter,
    RunEndEncodedType,
    STATS_KEY,
    Schema,
    SourceFile,
    StreamOwner,
    StructType,
    TimeType,
    TimestampType,
    UnionType,
    Utf8Type,
    ValidationError,
    ViewType,
    _ArrowTypesRoutedUnion,
    _ScanColumns,
    _VTable,
    _addscanrows,
    _arm_foreign_owner!,
    _batchcodec,
    _batchrows,
    _bitmapbytes,
    _blockmessage,
    _build_c_data!,
    _cleanup_export_slots!,
    _coalesce,
    _coremetatype,
    _decompressbuffer!,
    _export_stream!,
    _foreign_owner_armed,
    _framemessages,
    _from_c_data,
    _joinscanparts,
    _malloc!,
    _metaschema!,
    _newroot,
    _nextbatch!,
    _rangedfooter,
    _readstats,
    _recordbatchmeta,
    _release_array,
    _release_c_array!,
    _release_c_schema!,
    _release_foreign_owner!,
    _release_schema,
    _remaining,
    _requirelittleendian,
    _set_stream_error!,
    _statfold,
    _statsbatch,
    _statsschema,
    _store_field!,
    _storageelementclaim,
    _stream_get_next_impl,
    _stream_get_schema_impl,
    _stream_registry_count,
    _stream_state,
    _validateblockindex,
    _validatebodyplan,
    _vfield,
    _vi32,
    _vi64,
    _vrange,
    _vref,
    _vtable,
    _vu32,
    _vu8,
    _vvector,
    assigndictids,
    batch,
    export_stream!,
    formatstring,
    framemessages,
    from_c_data,
    from_c_stream,
    fromjulia,
    getvalue,
    heapregion,
    increment!,
    layoutspec,
    materialize,
    mmapregion,
    nextbatch!,
    nullcount,
    parseformat,
    readfile,
    readstream,
    reap!,
    release!,
    statsfile,
    to_c_data,
    validate_full,
    validate_semantic,
    validatedictionaryids,
    validaterecordcolumns,
    variadiccounts,
    verify_footer,
    verify_ipc_metadata,
    withstatistics,
    writefile,
    writestream

include(joinpath(@__DIR__, "cdata_stress.jl"))
include(joinpath(@__DIR__, "..", "battery_helpers.jl"))
include(joinpath(@__DIR__, "..", "ipc_read_battery.jl"))
include(joinpath(@__DIR__, "..", "ipc_write_battery.jl"))
include(joinpath(@__DIR__, "..", "cdata_battery.jl"))
include(joinpath(@__DIR__, "..", "scan_battery.jl"))

function _run_scan_acceptance()
    _stats_main()
    filebytes, af, full = _scan_main()
    _ranged_main(filebytes, af, full)
    return nothing
end

"The stable acceptance names and their throwing runners, in execution order."
acceptance_suites() = (
    "IPC read acceptance" => ipc_read_battery,
    "IPC write acceptance" => ipc_write_battery,
    "C data acceptance" => cdata_battery,
    "Scan acceptance" => _run_scan_acceptance,
)

end # module AcceptanceSupport
