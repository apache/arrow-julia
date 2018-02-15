# Arrow

This is a pure Julia implementation of the [Apache Arrow](https://arrow.apache.org) data standard.  This package provides Julia `AbstractVector` objects for
referencing data that conforms to the Arrow standard.  This allows users to seamlessly interface Arrow formatted data with a great deal of existing Julia code.


## `ArrowVector` Objects
The `Arrow` package exposes several `ArrowVector{J} <: AbstractVector{J}` objects.  These provide an interface to arrow formatted data as well as providing
methods to convert Julia objects to the Arrow data format.  The simplest of these is
```julia
Primitive{J} <: ArrowVector{J}
```
This object maintains a reference to a data buffer (a `Vector{UInt8}`) and describes and contiguous subset of it.  It will automatically convert the underlying
data to the type `J` on demand.  The `Primitive` type can only describe bits type elements (i.e. types for which `isbits` is true, in particular not strings).  In the
following example we create a `Primitive` to address a subset of a buffer
```julia
data = [0, 2, 3, 5, 7, 0] # this will be the underlying data from which we create our buffer
buff = reinterpret(UInt8, data) # in this simple case the Arrow format and Julia's in-memory format coincide
p = Primitive{Int}(buff, 9, 4) # arguments are: buffer, start location, length

p[1] # returns 2
p[2:3] # returns the (non-arrow) Vector [3,5]
p[:] # returns the (non-arrow) Vector [2,3,5,7]

p[2] = 999 # assignment is supported for AbstractPrimitive types. this change is reflected in buff and data


q = Primitive([2,3,5,7]) # if we didn't already have a buffer we needed to reference, we can create one like this
q = arrowformat([2,3,5,7]) # the arrowformat function automatically determines the appropriate ArrowVector for the provided array
rawvalues(q) # this returns the created buffer as a Vector{UInt8}
```
Here we see that indexing an `ArrowVector` returns ordinary Julia arrays containing the data stored in the Arrow buffer.  All other `ArrowVector` objects are
built out of combinations of `Primitive`s.

Enter `?Primitive` in the REPL for a full list of constructors.

### The `NullablePrimitive` Type
The Arrow format also supports arrays with bits type elements that may be null.  For these we provide the `NullablePrimitive{J} <: AbstractVector{Union{J,Missing}}` type.  Under the hood the
`NullablePrimitive` type is a pair of `Primitive`s: one references a `Primitive{UInt8}` bit mask describing which elements of the `NullablePrimitive` are null and the
other references the underlying data.  In the following example we create a `NullablePrimitive` from an existing buffer
```julia
buff = [[0x0d]; reinterpret(UInt8, [2.0, 3.0, 5.0, 7.0])]  # bits(0x0d) == "00001101"
p = NullablePrimitive{Float64}(buff, 1, 2, 4) # arguments are: buffer, bitmask location, values location, length

p[1] # returns 2.0
p[2] # returns missing
p[1:4] # returns [2.0, missing, 5.0, 7.0]

p[2] = 3.0  # assignment also supported for NullablePrimitive, the change will be reflected in buff


q = NullablePrimitive([2.0,missing,5.0,7.0]) # if we didn't already have a buffer we needed to reference, we can create one
# the above will create seperate buffers for the bit mask and values. to create a contiguous buffer containing all we can do
q = NullablePrimitive(Array, [2.0,missing,5.0,7.0])
q = arrowformat([2.0,missing,5.0,7.0]) # you can also use arrowformat to automatically determine the ArrowVector type
rawvalues(bitmask(q)) # returns [0x0d]
```

Enter `?NullablePrimitive` in the REPL for a full list of constructors.

### The `List` Type
The underlying dataformat for arbitrary length objects such as strings is more complicated, so these objects require a dedicated type.  For these we provide
`List{J} <: AbstractVector{J}`.  As well as containing the values contained by strings, these objects contain "offsets" for describing how long each string
should be.  The arrow format requires that these offsets are `Int32`s and that there are `length(l)+1` of them.  For example
```julia
offs = reinterpret(UInt8, Int32[0,3,5,7])
vals = convert(Vector{UInt8}, "abcdefg")
buff = [offs; vals]
l = List{String}(buff, 1, length(offs)+1, 3, UInt8, length(vals)) # arguments are: buffer, offsets location, values location, length of List, value type, values length

# alternatively we can construct the values separately
v = Primitive{UInt8}(buff, length(offs)+1, length(vals))
l = List{String}(buff, 1, 3, v) # arguments are: buffer, offset location, length, values primitive

# or you can create each piece individually
o = Primitive{Int32}(buff, 1, 4)  # note that the Int32 type is required for offsets by the arrow format
v = Primitive{UInt8}(buff, length(offs)+1, length(vals))
l = List{String}(o, v)

l[1] # returns "abc"
l[2] # returns "de"
l[3] # returns "fg"
l[1:3] # returns a normal Vector{String} (copies data!)

l[1] = "a"  # ERROR: assignments are not currently supported for list types


m = List(["abc", "de", "fg"]) # just as in the other cases, you can create your own data
m = List(Array, ["abc", "de", "fg"]) # you can also require it all to be in a contiguous buffer
m = arrowformat(["abc", "de", "fg"]) # as always arrowformat automatically determines the ArrowVector type
rawvalues(offsets(m)) # returns reinterpret(UInt8, [0,3,5,7])
rawvalues(values(m)) # returns convert(Vector{UInt8}, "abcdefg")
```
Note that `List{J}` and `NullableList{J}` use the constructor `J(::AbstractVector{C})` where `C` is the values type (in the above example `UInt8`)

Enter `?List` in the REPL for a full list of constructors.

### The `NullableList` Type
Next we have the `NullableList{J} <: AbstractVector{Union{J,Missing}}` type.  `NullableList` is to `List` as `NullablePrimitive` is to `Primitive`.  In addition
to offsets and values, it also contains a bit mask describing which elements are null.  By now you can probably predict what the example will look like
```julia
bmask = [0x05] # bits(0x05) == "00000101"
offs = reinterpret(UInt8, Int32[0,3,5,7])
vals = convert(Vector{UInt8}, "abcdefg")
buff = [bmask; offs; vals]
l = NullableList{String}(buff, 1, 2, length(offs)+2, 3, UInt8, length(vals))
# arguments above are: buffer, bit mask location, offsets location, values location, list length, values type, values length

# again you can also provide each piece separately
b = Primitive{UInt8}(buff, 1, 1)  # required to have eltype UInt8
o = Primitive{Int32}(buff, 2, 4)  # required to have eltype Int32
v = Primitive{UInt8}(buff, length(offs)+2, length(vals))
l = NullableList{String}(b, o, v)

l[1] # returns "abc"
l[2] # returns missing
l[3] # returns "fg"

l[2] = "de"  # ERROR assignments not currently supported for list types


# you can also create lists of Primitives, though this may involve copying
l = NullableList{Primitive{UInt8}}(b, o, v)


# by now all the ways of creating this from our own data should be familiar
m = NullableList(["abc", missing, "fg"])
m = NullableList(Array, ["abc", missing, "fg"])
m = arrowformat(["abc", missing, "fg"])
```

Enter `?NullableList` in the REPL for a full list of constructors.


## Recommended Usage Pattern
Because the Arrow standard is so general it is difficult for this package to provide general utilities for retrieving data.  Typically users will have to define
methods for creating `ArrowVector` objects from whatever underlying data that they are interested in.  The examples above demonstrate this, though of course in
most real use cases `buff` will be provided from a source such as reading in a file.  It is up to package developers to decide how the data should be
referenced.  Ideally, there should be some sort of metadata object which gives the locations of the various subbuffers.  For example, a package might define its
own constructors
```julia
function Arrow.NullablePrimitive(data::Vector{UInt8}, meta::ExamplePackage.ArrayMetadata{T}) where T
    NullablePrimitive{T}(data, bitmaskloc(meta), valuesloc(meta), length(meta))
end
```
In the above example the package developer has some metadata object which describes the properties of an arrow formatted array and uses it to create a
`NullablePrimitive`.  Presumably analogous methods would be defined for other `ArrowVector` types.  Of course the details of how this is done is entirely up to
the package developer.  As far as *reading* data goes, since `ArrowVector{J} <: AbstractVector{J}`, in many cases it shouldn't be necessary to convert the
`ArrowVector`s into Julia `Vector`s.  This will cut down on the amount of copying that needs to be done.  Of course if very fast access is a priority, nothing
will beat native Julia formats, so in these cases Julia `Vector`s should be constructed with `v[:]`.

***INCOMPLETE: More README is on its way!***
