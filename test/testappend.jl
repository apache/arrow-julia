function testappend(nm, t, writekw, readkw, extratests)
    println("testing append: $nm")
    io = Arrow.tobuffer(t; writekw...)
    bytes = read(io)
    mktemp() do path, io
        write(io, bytes)
        close(io)

        t1 = Arrow.Table(read(path); readkw...)
        f1 = first(Tables.columns(t1))
        Arrow.append(path, t1; writekw..., readkw...)
        nparts = 0
        for t2 in Arrow.Stream(path)
            @test isequal(f1, first(Tables.columns(t2)))
            nparts += 1
        end
        @test nparts == 2
    end
end

function testappend_compression(compression_option)
    mktempdir() do path
        testdata = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
        file1 = joinpath(path, "table1.arrow")
        file2 = joinpath(path, "table2.arrow")

        open(file1, "w") do io
            Arrow.write(io, testdata; file=false, compress=compression_option)
        end
        schema, compression = open(Arrow.table_info, file1)
        @test compression == compression_option

        open(file2, "w") do io
            Arrow.write(io, testdata; file=false)
        end

        arrow_table2 = Arrow.Table(file2)
        arrow_table2 |> Arrow.append(file1)
        arrow_table1 = Arrow.Table(file1)

        schema, compression = open(Arrow.table_info, file1)
        @test compression == compression_option

        @test length(Tables.columns(arrow_table1)[1]) == 20
        @test length(Tables.columns(arrow_table2)[1]) == 10
    end
end

function testappend_partitions()
    mktempdir() do path
        testdata = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
        file1 = joinpath(path, "table1.arrow")
        file2 = joinpath(path, "table2.arrow")
        open(file1, "w") do io
            Arrow.write(io, testdata; file=false)
        end
        arrow_table1 = Arrow.Table(file1)
        schema, compression = open(Arrow.table_info, file1)
        @test compression === nothing
        @test schema.names == (:col1,)
        @test schema.types == (Int64,)

        # can only append to arrow stream
        open(file2, "w") do io
            Arrow.write(io, testdata; file=true)
        end
        @test_throws ArgumentError Arrow.append(file2, arrow_table1)

        # schema must match
        testdata2 = (col2=Int64[1,2,3,4,5,6,7,8,9,10],)
        open(file2, "w") do io
            Arrow.write(io, testdata2; file=false)
        end
        @test_throws ArgumentError Arrow.append(file2, arrow_table1)

        # recreate file2 in arrow format with correct schema
        open(file2, "w") do io
            Arrow.write(io, testdata; file=false)
        end

        # start
        # arrow_table1: 1 partition, 10 rows
        # arrow_table2: 1 partition, 10 rows
        arrow_table2 = Arrow.Table(file2)
        @test length(Tables.columns(arrow_table1)[1]) == 10
        @test length(Tables.columns(arrow_table2)[1]) == 10

        @test_throws ArgumentError Arrow.append(file1, arrow_table2; ntasks = -1)
        arrow_table2 |> Arrow.append(file1)
        arrow_table1 = Arrow.Table(file1)
        # now
        # arrow_table1: 2 partitions, 20 rows
        # arrow_table2: 1 partition, 10 rows

        @test Tables.schema(arrow_table1) == Tables.schema(arrow_table2)
        @test length(Tables.columns(arrow_table1)[1]) == 20
        @test length(Tables.columns(arrow_table2)[1]) == 10
        @test length(collect(Tables.partitions(Arrow.Stream(file1)))) == 2 * length(collect(Tables.partitions(Arrow.Stream(file2))))

        Arrow.append(file2, arrow_table1; ntasks=1) # append with single task
        arrow_table2 = Arrow.Table(file2)
        # now
        # arrow_table1: 2 partitions, 20 rows
        # arrow_table2: 2 partitions, 30 rows (both partitions of table1 are appended as single partition)

        @test Tables.schema(arrow_table1) == Tables.schema(arrow_table2)
        @test length(Tables.columns(arrow_table1)[1]) == 20
        @test length(Tables.columns(arrow_table2)[1]) == 30
        @test length(collect(Tables.partitions(Arrow.Stream(file1)))) == length(collect(Tables.partitions(Arrow.Stream(file2))))

        Arrow.append(file1, Arrow.Stream(file2))
        arrow_table1 = Arrow.Table(file1)
        # now
        # arrow_table1: 4 partitions, 50 rows (partitions of file2 stream are appended without being merged)
        # arrow_table2: 2 partitions, 30 rows

        @test Tables.schema(arrow_table1) == Tables.schema(arrow_table2)
        @test length(Tables.columns(arrow_table1)[1]) == 50
        @test length(Tables.columns(arrow_table2)[1]) == 30
        @test length(collect(Tables.partitions(Arrow.Stream(file1)))) == 2 * length(collect(Tables.partitions(Arrow.Stream(file2))))
    end
end