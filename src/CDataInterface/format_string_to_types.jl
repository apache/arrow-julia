# https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings

function get_type_from_format_string(format_string ::AbstractString) ::Type
    # Primitives
    if format_string == "n"
        Nothing
    elseif format_string == "b"
        Bool
    elseif format_string == "c"
        Int8
    elseif format_string == "C"
        UInt8
    elseif format_string == "s"
        Int16
    elseif format_string == "S"
        UInt16
    elseif format_string == "i"
        Int32
    elseif format_string == "I"
        UInt32
    elseif format_string == "l"
        Int64
    elseif format_string == "L"
        UInt64
    elseif format_string == "e"
        Float16
    elseif format_string == "f"
        Float32
    elseif format_string == "g"
        Float64
    
    # Binary types
    elseif format_string == "z" || format_string == "Z"
        Vector{UInt8}
    elseif format_string == "u" || format_string == "U"
        String
    elseif format_string[1] == 'd'
        splits = Int.(split(format_string[3:end], ","))
        precision = splits[1]
        scale = splits[2]
        bitwidth = if (length(splits) == 3) splits[3] else 128 end
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

function parse_timezone(s ::AbstractString)
    
end
