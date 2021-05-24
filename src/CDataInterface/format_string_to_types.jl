# https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings

nullable = true

function convert_primitive(
            type ::Type,
            bitwidth ::Int,
            c_arrow_array ::InterimCArrowArray,
            c_arrow_schema ::ArrowSchema
        ) ::Arrow.ArrowVector
    length = c_arrow_array.length

    arrow_data_buffer = Base.unsafe_wrap(
        Array, 
        c_arrow_array.buffers[2], 
        cld(length * bitwidth, 8))
    validity_bytes = Base.unsafe_wrap(
        Array, 
        c_arrow_array.buffers[1], 
        cld(length, 8))
    validity_bitmap = Arrow.ValidityBitmap(
        validity_bytes, 
        1,  # Since we are not reading from a file, the start pos will always be 1
        length, 
        c_arrow_array.null_count)

    data = reinterpret(type, arrow_data_buffer)
    
    T = nullable ? Union{type, Missing} : type

    Arrow.Primitive{T, AbstractVector{T}}(
        arrow_data_buffer, 
        validity_bitmap, 
        data, 
        length, 
        c_arrow_schema.metadata)
end

function convert_to_string_vector(
            c_arrow_array ::InterimCArrowArray,
            c_arrow_schema ::ArrowSchema
        ) ::Arrow.ArrowVector
    
    length = c_arrow_array.length
    offsets_buffer_binary = Base.unsafe_wrap(
        Array, 
        c_arrow_array.buffers[2], 
        cld((length + 1) * 32, 8))
    offsets = Arrow.Offsets{Int32}(
        offsets_buffer_binary, 
        reinterpret(Int32, offsets_buffer_binary))

    arrow_data_buffer = Base.unsafe_wrap(Array, c_arrow_array.buffers[3], offsets |> last |> last)

    validity_bytes = Base.unsafe_wrap(Array, c_arrow_array.buffers[1], cld(length, 8))
    validity_bitmap = Arrow.ValidityBitmap(validity_bytes, 1, length, c_arrow_array.null_count)

    type = String
    T = nullable ? Union{type, Missing} : type

    return Arrow.List{T, Int32, AbstractVector{UInt8}}(
        arrow_data_buffer, 
        validity_bitmap, 
        offsets, 
        arrow_data_buffer, 
        length, 
        c_arrow_schema.metadata)
end

function convert_to_jl_arrow(
            c_arrow_array ::InterimCArrowArray, 
            c_arrow_schema ::ArrowSchema
        ) ::Arrow.ArrowVector

    format_string = c_arrow_schema.format
    # Primitives
    if format_string == "n"
        Nothing
    elseif format_string == "b"
        Bool
    elseif format_string == "c"
        convert_primitive(
            Int8,
            8,
            c_arrow_array,
            c_arrow_schema)
    elseif format_string == "C"
        convert_primitive(
            UInt8,
            8,
            c_arrow_array,
            c_arrow_schema)
    elseif format_string == "s"
        convert_primitive(
            Int16,
            16,
            c_arrow_array,
            c_arrow_schema)
    elseif format_string == "S"
        convert_primitive(
            UInt16,
            16,
            c_arrow_array,
            c_arrow_schema)
    elseif format_string == "i"
        convert_primitive(
            Int32,
            32,
            c_arrow_array,
            c_arrow_schema)
    elseif format_string == "I"
        convert_primitive(
            UInt32,
            32,
            c_arrow_array,
            c_arrow_schema)
    elseif format_string == "l"
        convert_primitive(
            Int64,
            64,
            c_arrow_array,
            c_arrow_schema)
    elseif format_string == "L"
        convert_primitive(
            UInt64,
            64,
            c_arrow_array,
            c_arrow_schema)
    elseif format_string == "e"
        convert_primitive(
            Float16,
            16,
            c_arrow_array,
            c_arrow_schema)
    elseif format_string == "f"
        convert_primitive(
            Float32,
            32,
            c_arrow_array,
            c_arrow_schema)
    elseif format_string == "g"
        convert_primitive(
            Float64,
            64,
            c_arrow_array,
            c_arrow_schema)
    
    # Binary types
    elseif format_string == "z" || format_string == "Z"
        Vector{UInt8}
    elseif format_string == "u" || format_string == "U"
        convert_to_string_vector(c_arrow_array, c_arrow_schema)
    elseif format_string[1] == 'd'
        splits = Int.(split(format_string[3:end], ","))
        precision = splits[1]
        scale = splits[2]
        bitwidth = length(splits) == 3 ? splits[3] : 128
        Decimal{precision, scale, bitwidth}
    elseif format_string[1] == 'w'
        Arrow.FixedSizeList{UInt8}

    # Nested Types
    elseif format_string[1] == '+'
        if format_string[2] == 'l' || format_string[2] == 'L'
            Arrow.List
        elseif format_string[2] == 'w'
            size = Int(format_string[4:end]) #TODO use this somehow
            Arrow.FixedSizeList
        elseif format_string[2] == 's'
            Arrow.Struct
        elseif format_string[2] == 'm'
            Arrow.Map
        elseif format_string[2:3] == "ud"
            type_strings = split(format_string[5:end], ",") # todo use this somehow
            Arrow.DenseUnion
        elseif format_string[2:3] == "us"
            type_strings = split(format_string[5:end], ",") # todo use this somehow
            Arrow.DenseUnion
        end

    # Temporal types
    elseif format_string[1] == 't'
        if format_string[2:3] == "dD"
            Arrow.Date{Arrow.Flatbuf.DateUnitModule.DAY, Int32}
        elseif format_string[2:3] == "dm"
            Arrow.Date{Arrow.Flatbuf.DateUnitModule.MILLISECOND, Int64}
        elseif format_string[2:3] == "ts"
            Arrow.Time{Arrow.Flatbuf.TimeUnitModule.SECOND, Int32}
        elseif format_string[2:3] == "tm"
            Arrow.Time{Arrow.Flatbuf.TimeUnitModule.MILLISECOND, Int32}
        elseif format_string[2:3] == "tu"
            Arrow.Time{Arrow.Flatbuf.TimeUnitModule.MICROSECOND, Int64}
        elseif format_string[2:3] == "tn"
            Arrow.Time{Arrow.Flatbuf.TimeUnitModule.NANOSECOND, Int64}
        elseif format_string[2] == 's'
            timestamp_unit = if format_string[3] == 's'
                Arrow.Flatbuf.TimeUnitModule.SECOND
            elseif format_string[3] == 'm'
                Arrow.Flatbuf.TimeUnitModule.MILLISECOND
            elseif format_string[3] == 'u'
                Arrow.Flatbuf.TimeUnitModule.MICROSECOND
            elseif format_string[3] == 'n'
                Arrow.Flatbuf.TimeUnitModule.NANOSECOND
            end

            timezone = length(format_string) == 4 ? nothing : format_string[5:end]

            Arrow.Timestamp{timestamp_unit, timezone}
        end
    end
end
