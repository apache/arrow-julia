function testappend()
    mktempdir() do path
        testdata = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
        file1 = joinpath(path, "table1.arrow")
        file2 = joinpath(path, "table2.arrow")
        open(file1, "w") do io
            Arrow.write(io, testdata; file=false)
        end
        open(file2, "w") do io
            Arrow.write(io, testdata; file=false)
        end

        # start
        # arrow_table1: 1 partition, 10 rows
        # arrow_table2: 1 partition, 10 rows
        arrow_table1 = Arrow.Table(file1)
        arrow_table2 = Arrow.Table(file2)
        @test length(Tables.columns(arrow_table1)[1]) == 10
        @test length(Tables.columns(arrow_table2)[1]) == 10

        Arrow.append(file1, arrow_table2)
        arrow_table1 = Arrow.Table(file1)
        # now
        # arrow_table1: 2 partitions, 20 rows
        # arrow_table2: 1 partition, 10 rows

        @test Tables.schema(arrow_table1) == Tables.schema(arrow_table2)
        @test length(Tables.columns(arrow_table1)[1]) == 20
        @test length(Tables.columns(arrow_table2)[1]) == 10
        @test length(collect(Tables.partitions(Arrow.Stream(file1)))) == 2 * length(collect(Tables.partitions(Arrow.Stream(file2))))

        Arrow.append(file2, arrow_table1)
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