# https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
module FormatStrings

function get_type_from_format_string(format_string ::String) ::DataType
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
    elseif format_string == "z" || format_string == "Z"
        Vector{UInt8}
    elseif format_string == "u" || format_string == "U"
        String
    elseif format_string[1] == 'd'
        const splits = split(format_string[3:end], ",")
        precision = Int(splits[1])
        scale = Int(splits[2])
        if length(splits) == 3
            bandwidth = splits[3]
        end
        #TODO return something here
    elseif format_string[1] == 'w'
        #TODO figure out fixed width binary
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
    elseif format_string[1] == 't'
        if format_string[2:3]
            Date
    end
end
    
end # module