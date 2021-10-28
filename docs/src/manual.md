# User Manual

The goal of this documentation is to provide a brief introduction to the arrow data format, then provide a walk-through of the functionality provided in the Arrow.jl Julia package, with an aim to expose a little of the machinery "under the hood" to help explain how things work and how that influences real-world use-cases for the arrow data format.

The best place to learn about the Apache arrow project is [the website itself](https://arrow.apache.org/), specifically the data format [specification](https://arrow.apache.org/docs/format/Columnar.html). Put briefly, the arrow project provides a formal speficiation for how columnar, "table" data can be laid out efficiently in memory to standardize and maximize the ability to share data across languages/platforms. In the current [apache/arrow GitHub repository](https://github.com/apache/arrow), language implementations exist for C++, Java, Go, Javascript, Rust, to name a few. Other database vendors and data processing frameworks/applications have also built support for the arrow format, allowing for a wide breadth of possibility for applications to "speak the data language" of arrow.

The [Arrow.jl](https://github.com/JuliaData/Arrow.jl) Julia package is another implementation, allowing the ability to both read and write data in the arrow format. As a data format, arrow specifies an exact memory layout to be used for columnar table data, and as such, "reading" involves custom Julia objects ([`Arrow.Table`](@ref) and [`Arrow.Stream`](@ref)), which read the *metadata* of an "arrow memory blob", then *wrap* the array data contained therein, having learned the type and size, amongst other properties, from the metadata. Let's take a closer look at what this "reading" of arrow memory really means/looks like.


## Reading arrow data

After installing the Arrow.jl Julia package (via `] add Arrow`), and if you have some arrow data, let's say a file named `data.arrow` generated from the [`pyarrow`](https://arrow.apache.org/docs/python/) library (a Python library for interfacing with arrow data), you can then read that arrow data into a Julia session by doing:

```julia
using Arrow

table = Arrow.Table("data.arrow")
```

### `Arrow.Table`

The type of `table` in this example will be an `Arrow.Table`. When "reading" the arrow data, `Arrow.Table` first ["mmapped"](https://en.wikipedia.org/wiki/Mmap) the `data.arrow` file, which is an important technique for dealing with data larger than available RAM on a system. By "mmapping" a file, the OS doesn't actually load the entire file contents into RAM at the same time, but file contents are "swapped" into RAM as different regions of a file are requested. Once "mmapped", `Arrow.Table` then inspected the metadata in the file to determine the number of columns, their names and types, at which byte offset each column begins in the file data, and even how many "batches" are included in this file (arrow tables may be partitioned into one or more "record batches" each containing portions of the data). Armed with all the appropriate metadata, `Arrow.Table` then created custom array objects ([`ArrowVector`](@ref)), which act as "views" into the raw arrow memory bytes. This is a significant point in that no extra memory is allocated for "data" when reading arrow data. This is in contrast to if we wanted to read data from a csv file as columns into Julia structures; we would need to allocate those array structures ourselves, then parse the file, "filling in" each element of the array with the data we parsed from the file. Arrow data, on the other hand, is *already laid out in memory or on disk* in a binary format, and as long as we have the metadata to interpret the raw bytes, we can figure out whether to treat those bytes as a `Vector{Float64}`, etc. A sample of the kinds of arrow array types you might see when deserializing arrow data, include:

* [`Arrow.Primitive`](@ref): the most common array type for simple, fixed-size elements like integers, floats, time types, and decimals
* [`Arrow.List`](@ref): an array type where its own elements are also arrays of some kind, like string columns, where each element can be thought of as an array of characters
* [`Arrow.FixedSizeList`](@ref): similar to the `List` type, but where each array element has a fixed number of elements itself; you can think of this like a `Vector{NTuple{N, T}}`, where `N` is the fixed-size width
* [`Arrow.Map`](@ref): an array type where each element is like a Julia `Dict`; a list of key value pairs like a `Vector{Dict}`
* [`Arrow.Struct`](@ref): an array type where each element is an instance of a custom struct, i.e. an ordered collection of named & typed fields, kind of like a `Vector{NamedTuple}`
* [`Arrow.DenseUnion`](@ref): an array type where elements may be of several different types, stored compactly; can be thought of like `Vector{Union{A, B}}`
* [`Arrow.SparseUnion`](@ref): another array type where elements may be of several different types, but stored as if made up of identically lengthed child arrays for each possible type (less memory efficient than `DenseUnion`)
* [`Arrow.DictEncoded`](@ref): a special array type where values are "dictionary encoded", meaning the list of unique, possible values for an array are stored internally in an "encoding pool", whereas each stored element of the array is just an integer "code" to index into the encoding pool for the actual value.

And while these custom array types do subtype `AbstractArray`, there is no current support for `setindex!`. Remember, these arrays are "views" into the raw arrow bytes, so for array types other than `Arrow.Primitive`, it gets pretty tricky to allow manipulating those raw arrow bytes. Nevetheless, it's as simple as calling `copy(x)` where `x` is any `ArrowVector` type, and a normal Julia `Vector` type will be fully materialized (which would then allow mutating/manipulating values).

So, what can you do with an `Arrow.Table` full of data? Quite a bit actually!

Because `Arrow.Table` implements the [Tables.jl](https://juliadata.github.io/Tables.jl/stable/) interface, it opens up a world of integrations for using arrow data. A few examples include:

* `df = DataFrame(Arrow.Table(file))`: Build a [`DataFrame`](https://juliadata.github.io/DataFrames.jl/stable/), using the arrow vectors themselves; this allows utilizing a host of DataFrames.jl functionality directly on arrow data; grouping, joining, selecting, etc.
* `Tables.datavaluerows(Arrow.Table(file)) |> @map(...) |> @filter(...) |> DataFrame`: use [`Query.jl`'s](https://www.queryverse.org/Query.jl/stable/standalonequerycommands/) row-processing utilities to map, group, filter, mutate, etc. directly over arrow data.
* `Arrow.Table(file) |> SQLite.load!(db, "arrow_table")`: load arrow data directly into an sqlite database/table, where sql queries can be executed on the data
* `Arrow.Table(file) |> CSV.write("arrow.csv")`: write arrow data out to a csv file

A full list of Julia packages leveraging the Tables.jl inteface can be found [here](https://github.com/JuliaData/Tables.jl/blob/master/INTEGRATIONS.md).

Apart from letting other packages have all the fun, an `Arrow.Table` itself can be plenty useful. For example, with `tbl = Arrow.Table(file)`:
* `tbl[1]`: retrieve the first column via indexing; the number of columns can be queried via `length(tbl)`
* `tbl[:col1]` or `tbl.col1`: retrieve the column named `col1`, either via indexing with the column name given as a `Symbol`, or via "dot-access"
* `for col in tbl`: iterate through columns in the table
* `AbstractDict` methods like `haskey(tbl, :col1)`, `get(tbl, :col1, nothing)`, `keys(tbl)`, or `values(tbl)`

### Arrow types

In the arrow data format, specific logical types are supported, a list of which can be found [here](https://arrow.apache.org/docs/status.html#data-types). These include booleans, integers of various bit widths, floats, decimals, time types, and binary/string. While most of these map naturally to types builtin to Julia itself, there are a few cases where the definitions are slightly different, and in these cases, by default, they are converted to more "friendly" Julia types (this auto conversion can be avoided by passing `convert=false` to `Arrow.Table`, like `Arrow.Table(file; convert=false)`). Examples of arrow to julia type mappings include:

* `Date`, `Time`, `Timestamp`, and `Duration` all have natural Julia defintions in `Dates.Date`, `Dates.Time`, `TimeZones.ZonedDateTime`, and `Dates.Period` subtypes, respectively.
* `Char` and `Symbol` Julia types are mapped to arrow string types, with additional metadata of the original Julia type; this allows deserializing directly to `Char` and `Symbol` in Julia, while other language implementations will see these columns as just strings
* Similarly to the above, the `UUID` Julia type is mapped to a 128-bit `FixedSizeBinary` arrow type.
* `Decimal128` and `Decimal256` have no corresponding builtin Julia types, so they're deserialized using a compatible type definition in Arrow.jl itself: `Arrow.Decimal`


Note that when `convert=false` is passed, data will be returned in Arrow.jl-defined types that exactly match the arrow definitions of those types; the authoritative source for how each type represents its data can be found in the arrow [`Schema.fbs`](https://github.com/apache/arrow/blob/master/format/Schema.fbs) file.

One note on performance: when writing `TimeZones.ZonedDateTime` columns to the arrow format (via `Arrow.write`), it is preferrable to "wrap" the columns in `Arrow.ToTimestamp(col)`, as long
as the column has `ZonedDateTime` elements that all share a common timezone. This ensures the writing process can know "upfront" which timezone will be encoded and is thus much more
efficient and performant.

#### Custom types

To support writing your custom Julia struct, Arrow.jl utilizes the format's mechanism for "extension types" by allowing the storing of Julia type name and metadata in the field metadata. To "hook in" to this machinery, custom types can utilize the interface methods defined in the `Arrow.ArrowTypes` submodule. For example:

```julia
using Arrow

struct Person
    id::Int
    name::String
end

# overload interface method for custom type Person; return a symbol as the "name"
# this instructs Arrow.write what "label" to include with a column with this custom type
ArrowTypes.arrowname(::Type{Person}) = :Person
# overload JuliaType on `Val{:Person}`, which is like a dispatchable string
# return our custom *type* Person; this enables Arrow.Table to know how the "label"
# on a custom column should be mapped to a Julia type and deserialized
ArrowTypes.JuliaType(::Val{:Person}) = Person

table = (col1=[Person(1, "Bob"), Person(2, "Jane")],)
io = IOBuffer()
Arrow.write(io, table)
seekstart(io)
table2 = Arrow.Table(io)
```

In this example, we're writing our `table`, which is a NamedTuple with one column named `col1`, which has two
elements which are instances of our custom `Person` struct. We overload `Arrowtypes.arrowname` so that
Arrow.jl knows how to serialize our `Person` struct. We then overload `ArrowTypes.JuliaType` so the deserialization process knows how to map from our type label back to our `Person` struct type. We can then write our data in the arrow format to an in-memory `IOBuffer`, then read the table back in using `Arrow.Table`.
The table we get back will be an `Arrow.Table`, with a single `Arrow.Struct` column with element type `Person`.

Note that without calling `Arrowtypes.JuliaType`, we may get into a weird limbo state where we've written
our table with `Person` structs out as a table, but when reading back in, Arrow.jl doesn't know what a `Person` is;
deserialization won't fail, but we'll just get a `Namedtuple{(:id, :name), Tuple{Int, String}}` back instead of `Person`.

While this example is very simple, it shows the basics to allow a custom type to be serialized/deserialized. But the `ArrowTypes` module offers even more powerful functionality for "hooking" non-native arrow types into the serialization/deserialization processes. Let's walk through a couple more examples; if you've had enough custom type shenanigans, feel free to skip to the next section.

Let's take a look at how Arrow.jl allows serializing the `nothing` value, which is often referred to as the "software engineer's NULL" in Julia. While Arrow.jl treats `missing` as the default arrow NULL value, `nothing` is pretty similar, but we'd still like to treat it separately if possible. Here's how we enable serialization/deserialization in the `ArrowTypes` module:

```julia
ArrowTypes.ArrowKind(::Type{Nothing}) = ArrowTypes.NullKind()
ArrowTypes.ArrowType(::Type{Nothing}) = Missing
ArrowTypes.toarrow(::Nothing) = missing
const NOTHING = Symbol("JuliaLang.Nothing")
ArrowTypes.arrowname(::Type{Nothing}) = NOTHING
ArrowTypes.JuliaType(::Val{NOTHING}) = Nothing
ArrowTypes.fromarrow(::Type{Nothing}, ::Missing) = nothing
```

Let's walk through what's going on here, line-by-line:
  * `ArrowKind` overload: `ArrowKind`s are generic "categories" of types supported by the arrow format, like `PrimitiveKind`, `ListKind`, etc. They each correspond to a different data layout strategy supported in the arrow format. Here, we define `nothing`'s kind to be `NullKind`, which means no actual memory is needed for storage, it's strictly a "metadata" type where we store the type and # of elements. In our `Person` example, we didn't need to overload this since types declared like `struct T` or `mutable struct T` are defined as `ArrowTypes.StructKind` by default
  * `ArrowType` overload: here we're signaling that our type (`Nothing`) maps to the natively supported arrow type of `Missing`; this is important for the serializer so it knows which arrow type it will be serializing. Again, we didn't need to overload this for `Person` since the serializer knows how to serialize custom structs automatically by using reflection methods like `fieldnames(T)` and `getfield(x, i)`.
  * `ArrowTypes.toarrow` overload: this is a sister method to `ArrowType`; we said our type will map to the `Missing` arrow type, so here we actually define ___how___ it converts to the arrow type; and in this case, it just returns `missing`. This is yet another method that didn't show up for `Person`; why? Well, as we noted in `ArrowType`, the serializer already knows how to serialize custom structs by using all their fields; if, for some reason, we wanted to omit some fields or otherwise transform things, then we could define corresponding `ArrowType` and `toarrow` methods
  * `arrowname` overload: similar to our `Person` example, we need to instruct the serializer how to label our custom type in the arrow type metadata; here we give it the symbol `Symbol("JuliaLang.Nothing")`. Note that while this will ultimately allow us to disambiguate `nothing` from `missing` when reading arrow data, if we pass this data to other language implementations, they will only treat the data as `missing` since they (probably) won't know how to "understand" the `JuliaLang.Nothing` type label
  * `JuliaType` overload: again, like our `Person` example, we instruct the deserializer that when it encounters the `JuliaLang.Nothing` type label, it should treat those values as `Nothing` type.
  * And finally, `fromarrow` overload: this allows specifying how the native-arrow data should be converted back to our custom type. `fromarrow(T, x...)` by default will call `T(x...)`, which is why we didn't need this overload for `Person`, but in this example, `Nothing(missing)` won't work, so we define our own custom conversion.

Let's run through one more complex example, just for fun and to really see how far the system can be pushed:

```julia
using Intervals
table = (col = [
    Interval{Closed,Unbounded}(1,nothing),
],)
const NAME = Symbol("JuliaLang.Interval")
ArrowTypes.arrowname(::Type{Interval{T, L, R}}) where {T, L, R} = NAME
const LOOKUP = Dict(
    "Closed" => Closed,
    "Unbounded" => Unbounded
)
ArrowTypes.arrowmetadata(::Type{Interval{T, L, R}}) where {T, L, R} = string(L, ".", R)
function ArrowTypes.JuliaType(::Val{NAME}, ::Type{NamedTuple{names, types}}, meta) where {names, types}
    L, R = split(meta, ".")
    return Interval{fieldtype(types, 1), LOOKUP[L], LOOKUP[R]}
end
ArrowTypes.fromarrow(::Type{Interval{T, L, R}}, first, last) where {T, L, R} = Interval{L, R}(first, R == Unbounded ? nothing : last)
io = Arrow.tobuffer(table)
tbl = Arrow.Table(io)
```

Again, let's break down what's going on here:
  * Here we're trying to save an `Interval` type in the arrow format; this type is unique in that it has two type parameters (`Closed` and `Unbounded`) that are not inferred/based on fields, but are just "type tags" on the type itself
  * Note that we define a generic `arrowname` method on all `Interval`s, regardless of type parameters. We just want to let arrow know which general type we're dealing with here
  * Next we use a new method `ArrowTypes.arrowmetadata` to encode the two non-field-based type parameters as a string with a dot delimiter; we encode this information here because remember, we have to match our `arrowname` Symbol typename in our `JuliaType(::Val(name))` definition in order to dispatch correctly; if we encoded the type parameters in `arrowname`, we would need separate `arrowname` definitions for each unique combination of those two type parameters, and corresponding `JuliaType` definitions for each as well; yuck. Instead, we let `arrowname` be generic to our type, and store the type parameters *for this specific column* using `arrowmetadata`
  * Now in `JuliaType`, note we're using the 3-argument overload; we want the `NamedTuple` type that is the native arrow type our `Interval` is being serialized as; we use this to retrieve the 1st type parameter for our `Interval`, which is simply the type of the two `first` and `last` fields. Then we use the 3rd argument, which is whatever string we returned from `arrowmetadata`. We call `L, R = split(meta, ".")` to parse the two type parameters (in this case `Closed` and `Unbounded`), then do a lookup on those strings from a predefined `LOOKUP` Dict that matches the type parameter name as string to the actual type. We then have all the information to recreate the full `Interval` type. Neat!
  * The one final wrinkle is in our `fromarrow` method; `Interval`s that are `Unbounded`, actually take `nothing` as the 2nd argument. So letting the default `fromarrow` definition call `Interval{T, L, R}(first, last)`, where `first` and `last` are both integers isn't going to work. Instead, we check if the `R` type parameter is `Unbounded` and if so, pass `nothing` as the 2nd arg, otherwise we can pass `last`.

This stuff can definitely make your eyes glaze over if you stare at it long enough. As always, don't hesitate to reach out for quick questions on the [#data](https://julialang.slack.com/messages/data/) slack channel, or [open a new issue](https://github.com/JuliaData/Arrow.jl/issues/new) detailing what you're trying to do.

### `Arrow.Stream`

In addition to `Arrow.Table`, the Arrow.jl package also provides `Arrow.Stream` for processing arrow data. While `Arrow.Table` will iterate all record batches in an arrow file/stream, concatenating columns, `Arrow.Stream` provides a way to *iterate* through record batches, one at a time. Each iteration yields an `Arrow.Table` instance, with columns/data for a single record batch. This allows, if so desired, "batch processing" of arrow data, one record batch at a time, instead of creating a single long table via `Arrow.Table`.

### Custom application metadata

The Arrow format allows data producers to [attach custom metadata](https://arrow.apache.org/docs/format/Columnar.html#custom-application-metadata) to various Arrow objects.

Arrow.jl provides a convenient accessor for this metadata via [`Arrow.getmetadata`](@ref). `Arrow.getmetadata(t::Arrow.Table)` will return an immutable `AbstractDict{String,String}` that represents the [`custom_metadata` of the table's associated `Schema`](https://github.com/apache/arrow/blob/85d8175ea24b4dd99f108a673e9b63996d4f88cc/format/Schema.fbs#L515) (or `nothing` if no such metadata exists), while `Arrow.getmetadata(c::Arrow.ArrowVector)` will return a similar representation of [the column's associated `Field` `custom_metadata`](https://github.com/apache/arrow/blob/85d8175ea24b4dd99f108a673e9b63996d4f88cc/format/Schema.fbs#L480) (or `nothing` if no such metadata exists).

To attach custom schema/column metadata to Arrow tables at serialization time, see the `metadata` and `colmetadata` keyword arguments to [`Arrow.write`](@ref).

## Writing arrow data

Ok, so that's a pretty good rundown of *reading* arrow data, but how do you *produce* arrow data? Enter `Arrow.write`.

### `Arrow.write`

With `Arrow.write`, you provide either an `io::IO` argument or a `file_path` to write the arrow data to, as well as a Tables.jl-compatible source that contains the data to be written.

What are some examples of Tables.jl-compatible sources? A few examples include:
* `Arrow.write(io, df::DataFrame)`: A `DataFrame` is a collection of indexable columns
* `Arrow.write(io, CSV.File(file))`: read data from a csv file and write out to arrow format
* `Arrow.write(io, DBInterface.execute(db, sql_query))`: Execute an SQL query against a database via the [`DBInterface.jl`](https://github.com/JuliaDatabases/DBInterface.jl) interface, and write the query resultset out directly in the arrow format. Packages that implement DBInterface include [SQLite.jl](https://juliadatabases.github.io/SQLite.jl/stable/), [MySQL.jl](https://juliadatabases.github.io/MySQL.jl/dev/), and [ODBC.jl](http://juliadatabases.github.io/ODBC.jl/latest/).
* `df |> @map(...) |> Arrow.write(io)`: Write the results of a [Query.jl](https://www.queryverse.org/Query.jl/stable/) chain of operations directly out as arrow data
* `jsontable(json) |> Arrow.write(io)`: Treat a json array of objects or object of arrays as a "table" and write it out as arrow data using the [JSONTables.jl](https://github.com/JuliaData/JSONTables.jl) package
* `Arrow.write(io, (col1=data1, col2=data2, ...))`: a `NamedTuple` of `AbstractVector`s or an `AbstractVector` of `NamedTuple`s are both considered tables by default, so they can be quickly constructed for easy writing of arrow data if you already have columns of data

And these are just a few examples of the numerous [integrations](https://github.com/JuliaData/Tables.jl/blob/master/INTEGRATIONS.md).

In addition to just writing out a single "table" of data as a single arrow record batch, `Arrow.write` also supports writing out multiple record batches when the input supports the `Tables.partitions` functionality. One immediate, though perhaps not incredibly useful example, is `Arrow.Stream`. `Arrow.Stream` implements `Tables.partitions` in that it iterates "tables" (specifically `Arrow.Table`), and as such, `Arrow.write` will iterate an `Arrow.Stream`, and write out each `Arrow.Table` as a separate record batch. Another important point for why this example works is because an `Arrow.Stream` iterates `Arrow.Table`s that all have the same schema. This is important because when writing arrow data, a "schema" message is always written first, with all subsequent record batches written with data matching the initial schema.

In addition to inputs that support `Tables.partitions`, note that the Tables.jl itself provides the `Tables.partitioner` function, which allows providing your own separate instances of similarly-schema-ed tables as "partitions", like:

```julia
# treat 2 separate NamedTuples of vectors with same schema as 1 table, 2 partitions
tbl_parts = Tables.partitioner([(col1=data1, col2=data2), (col1=data3, col2=data4)])
Arrow.write(io, tbl_parts)

# treat an array of csv files with same schema where each file is a partition
# in this form, a function `CSV.File` is applied to each element of 2nd argument
csv_parts = Tables.partitioner(CSV.File, csv_files)
Arrow.write(io, csv_parts)
```

### Multithreaded writing

By default, `Arrow.write` will use multiple threads to write multiple
record batches simultaneously (e.g. if julia is started with `julia -t 8` or the `JULIA_NUM_THREADS` environment variable is set). The number of concurrent tasks to use when writing can be controlled by passing the `ntasks` keyword argument to `Arrow.write`. Passing `ntasks=1` avoids any multithreading when writing.

### Compression

Compression is supported when writing via the `compress` keyword argument. Possible values include `:lz4`, `:zstd`, or your own initialized `LZ4FrameCompressor` or `ZstdCompressor` objects; will cause all buffers in each record batch to use the respective compression encoding or compressor.
