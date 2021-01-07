
files = [
    "Arrow/docs/src/index.md",
    "Arrow/docs/src/manual.md",
    "Arrow/docs/src/reference.md",
    "Arrow/docs/make.jl",
    "Arrow/docs/Project.toml",
    "Arrow/src/arraytypes/arraytypes.jl",
    "Arrow/src/arraytypes/bool.jl",
    "Arrow/src/arraytypes/compressed.jl",
    "Arrow/src/arraytypes/dictencoding.jl",
    "Arrow/src/arraytypes/fixedsizelist.jl",
    "Arrow/src/arraytypes/list.jl",
    "Arrow/src/arraytypes/map.jl",
    "Arrow/src/arraytypes/primitive.jl",
    "Arrow/src/arraytypes/struct.jl",
    "Arrow/src/arraytypes/unions.jl",
    "Arrow/src/FlatBuffers/builder.jl",
    "Arrow/src/FlatBuffers/builder.jl",
    "Arrow/src/FlatBuffers/FlatBuffers.jl",
    "Arrow/src/FlatBuffers/table.jl",
    "Arrow/src/metadata/File.jl",
    "Arrow/src/metadata/Flatbuf.jl",
    "Arrow/src/metadata/Message.jl",
    "Arrow/src/metadata/Schema.jl",
    "Arrow/src/Arrow.jl",
    "Arrow/src/arrowtypes.jl",
    "Arrow/src/eltypes.jl",
    "Arrow/src/table.jl",
    "Arrow/src/utils.jl",
    "Arrow/src/write.jl",
    "Arrow/test/arrowjson/datetime.json",
    "Arrow/test/arrowjson/decimal.json",
    "Arrow/test/arrowjson/dictionary_unsigned.json",
    "Arrow/test/arrowjson/dictionary.json",
    "Arrow/test/arrowjson/map.json",
    "Arrow/test/arrowjson/nested.json",
    "Arrow/test/arrowjson/primitive_no_batches.json",
    "Arrow/test/arrowjson/primitive-empty.json",
    "Arrow/test/arrowjson/primitive.json",
    "Arrow/test/arrowjson.jl",
    "Arrow/test/dates.jl",
    "Arrow/test/integrationtest.jl",
    "Arrow/test/pyarrow_roundtrip.jl",
    "Arrow/test/runtests.jl",
    "Arrow/test/testtables.jl",
    "Arrow/.gitignore",
    "Arrow/LICENSE.md",
    "Arrow/Project.toml",
    # "Arrow/README.md", READMEs are slightly different between officially apache/arrow julia code and JuliaData/Arrow.jl
]

sourcedir = "/Users/jacobquinn/.julia/dev/"
destdir = "/Users/jacobquinn/arrow/julia/"

for file in files
    cp(joinpath(sourcedir, file), joinpath(destdir, file); force=true)
end