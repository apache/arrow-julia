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

# Seeded end-to-end properties over the public IPC read/write paths, driven
# by the SeededFuzz generators; included from runtests.jl.

module PropertyTests

using Arrow
using Test

include(joinpath(@__DIR__, "support", "SeededFuzz.jl"))
import .SeededFuzz

const SEED = SeededFuzz.DEFAULT_SEED

@testset "public IPC properties (seed=$(string(SEED; base=16)))" begin
    @testset "fuzz failure artifacts are complete and shell-safe" begin
        @test SeededFuzz._REPLAY_SCRIPT == "sh ./replay.sh /path/to/Arrow.jl"
        artifact = "reproductions/space and 'quote'\n`literal`.arrowbytes"
        @test SeededFuzz._shellquote("a b'c\n`d`") == "'a b'\"'\"'c\n`d`'"
        entry = SeededFuzz.MutationCorpus(:fixture, false, UInt8[])
        text = SeededFuzz._mutation_repro_text(
            SEED,
            UInt64(0x1234),
            7,
            entry,
            :stream_full,
            "flip[1]=0x01",
            artifact,
            "failure details",
        )
        for required in (
            "julia_version=",
            "source_revision=",
            "master_seed=",
            "case_seed=",
            "index=7",
            "corpus=fixture",
            "lane=stream_full",
            "recipe=flip[1]=0x01",
            "replay_generated=$(SeededFuzz._REPLAY_SCRIPT)",
            "replay_bytes=$(SeededFuzz._REPLAY_SCRIPT)",
            "--mutation-index 7 --skip-layouts",
            "--mutation-file $(SeededFuzz._shellquote(basename(artifact)))",
            "failure details",
        )
            @test occursin(required, text)
        end
        @test !occursin(dirname(artifact), text)

        caseoptions = SeededFuzz.parse_options([
            "--case-index",
            "7",
            "--cases",
            "99",
            "--mutations",
            "99",
        ])
        @test caseoptions.case_index == 7
        @test caseoptions.mutation_index === nothing
        @test caseoptions.cases == caseoptions.mutations == 0
        @test !caseoptions.include_layouts
        @test !caseoptions.verify_determinism
        @test caseoptions.determinism_every == 0

        mutationoptions =
            SeededFuzz.parse_options(["--mutation-index=9", "--determinism-every=256"])
        @test mutationoptions.case_index === nothing
        @test mutationoptions.mutation_index == 9
        @test mutationoptions.cases == mutationoptions.mutations == 0
        @test !mutationoptions.include_layouts
        @test mutationoptions.verify_determinism
        @test mutationoptions.determinism_every == 256
        @test SeededFuzz._shouldverify(1, 41, 256)
        @test SeededFuzz._shouldverify(41, 41, 256)
        @test !SeededFuzz._shouldverify(42, 41, 256)
        @test SeededFuzz._shouldverify(256, 41, 256)

        outcome = SeededFuzz._classify_mutation(UInt8[], :stream_full)
        @test outcome.category === :validation
        @test outcome.error isa Arrow.ValidationError

        statsentry = only(
            entry for entry in SeededFuzz._mutationcorpus() if entry.label === :stats_file
        )
        statssnapshot = SeededFuzz._exercise_mutation(statsentry.bytes, :stats_filtered)
        @test statssnapshot.nrows == 2
        @test statssnapshot.names == [:id]
        @test statssnapshot.eltypes == Type[Int64]
        @test only(statssnapshot.values) == Int64[4, 5]
        statsfile = Arrow.readfile(copy(statsentry.bytes))
        recordspans = [(block[1], block[2] + block[3]) for block in statsfile.recordblocks]
        @test !any(
            request -> SeededFuzz._rangesintersect(request, recordspans[1]),
            statssnapshot.requests,
        )
        @test any(
            request -> SeededFuzz._rangesintersect(request, recordspans[2]),
            statssnapshot.requests,
        )
        @test !any(
            request -> request == (Int64(0), Int64(length(statsentry.bytes))),
            statssnapshot.requests,
        )
        renamed = copy(statsentry.bytes)
        namestarts = findall(
            index -> renamed[index] == UInt8('i') && renamed[index + 1] == UInt8('d'),
            1:(length(renamed) - 1),
        )
        @test length(namestarts) == 2
        for index in namestarts
            renamed[index] = UInt8('j')
        end
        renamedoutcome = SeededFuzz._classify_mutation(renamed, :stats_filtered)
        @test renamedoutcome.category === :accepted
        @test first(renamedoutcome.snapshot.names) === :jd

        base = UInt8[0x00, 0x01, 0xff]
        for seed = UInt64(1):UInt64(256), operation in SeededFuzz._MUTATION_OPERATIONS
            mutated, _, _ =
                SeededFuzz._mutatebytes(base, SeededFuzz.StableRNG(seed), operation)
            @test mutated != base
        end

        mktempdir() do dir
            active = joinpath(dir, "active")
            mkpath(active)
            project = joinpath(active, "Project.toml")
            projecttext = "[deps]\n"
            manifesttext =
                "julia_version = \"$(VERSION)\"\n" *
                "manifest_format = \"2.0\"\n" *
                "project_hash = \"0000000000000000000000000000000000000000\"\n"
            write(project, projecttext)
            write(joinpath(active, "Manifest.toml"), manifesttext)
            artifact = joinpath(dir, "artifact-original")
            SeededFuzz._snapshot_environment(artifact, project)
            moved = joinpath(dir, "artifact-renamed")
            mv(artifact, moved)
            @test isfile(joinpath(moved, "environment", "Project.toml"))
            @test isfile(joinpath(moved, "environment", "Manifest.toml"))
            @test isfile(joinpath(moved, "replay.sh"))
            replaytext = read(joinpath(moved, "replay.sh"), String)
            @test occursin("Pkg.instantiate()", replaytext)
            restore = read(joinpath(moved, "environment", "RESTORE.txt"), String)
            @test startswith(restore, "environment_restore=")
            @test !occursin(dir, restore)

            checkout = joinpath(dir, "checkout")
            mkpath(joinpath(checkout, "test"))
            write(joinpath(checkout, "Project.toml"), "old project\n")
            write(joinpath(checkout, "Manifest.toml"), "old manifest\n")
            write(joinpath(checkout, "test", "fuzz.jl"), "")
            if !Sys.iswindows()
                observedproject = joinpath(dir, "observed-project")
                observedmanifest = joinpath(dir, "observed-manifest")
                fakejulia = joinpath(dir, "fake-julia")
                write(
                    fakejulia,
                    "#!/bin/sh\ncp \"\$ARROW_REPLAY_CHECKOUT/Project.toml\" " *
                    "\"\$ARROW_REPLAY_PROJECT\"\n" *
                    "cp \"\$ARROW_REPLAY_CHECKOUT/Manifest.toml\" " *
                    "\"\$ARROW_REPLAY_MANIFEST\"\n" *
                    "for arg in \"\$@\"; do\n" *
                    "    [ \"\$arg\" = -e ] && exit 0\n" *
                    "done\n" *
                    "exit \"\${ARROW_REPLAY_EXIT:-0}\"\n",
                )
                chmod(fakejulia, 0o700)
                replaycommand = Cmd([
                    "sh",
                    joinpath(moved, "replay.sh"),
                    checkout,
                    "--cases",
                    "0",
                    "--mutations",
                    "0",
                    "--skip-layouts",
                ])
                command = addenv(
                    replaycommand,
                    "JULIA" => fakejulia,
                    "ARROW_REPLAY_CHECKOUT" => checkout,
                    "ARROW_REPLAY_PROJECT" => observedproject,
                    "ARROW_REPLAY_MANIFEST" => observedmanifest,
                )
                run(command)
                @test read(observedproject, String) == projecttext
                @test read(observedmanifest, String) == manifesttext
                @test read(joinpath(checkout, "Project.toml"), String) == "old project\n"
                @test read(joinpath(checkout, "Manifest.toml"), String) == "old manifest\n"
                failed = run(addenv(command, "ARROW_REPLAY_EXIT" => "7"); wait=false)
                wait(failed)
                @test failed.exitcode == 7
                @test read(joinpath(checkout, "Project.toml"), String) == "old project\n"
                @test read(joinpath(checkout, "Manifest.toml"), String) == "old manifest\n"
            end
            SeededFuzz._save_active_repro(moved, :mutation, "active coordinate"; bytes=base)
            @test read(SeededFuzz._active_repro_path(moved, :mutation, "txt"), String) ==
                  "active coordinate"
            @test read(SeededFuzz._active_repro_path(moved, :mutation, "arrowbytes")) ==
                  base
            SeededFuzz._clear_active_repro(moved, :mutation)
            @test sort(readdir(moved)) == ["environment", "replay.sh"]
        end
    end

    @testset "declared struct child types survive empty and all-missing values" begin
        Row = @NamedTuple{x::Int16, label::Union{Missing,String}}
        for rows in (Row[], Row[(x=1, label=missing), (x=2, label=missing)])
            io = IOBuffer()
            Arrow.write(io, (structs=rows,); file=false)
            bytes = take!(io)
            stream = Arrow.readstream(bytes)
            children = stream.schema.fields[1].children
            @test children[1].type == Arrow.AC.IntType(16, true)
            @test !children[1].nullable
            @test children[2].type == Arrow.AC.Utf8Type(false)
            @test children[2].nullable
            table = Arrow.Table(bytes)
            @test isequal(
                collect(table.structs),
                [["x" => row.x, "label" => row.label] for row in rows],
            )
        end

        EmptyRow = NamedTuple{(),Tuple{}}
        for rows in (EmptyRow[], EmptyRow[(;), (;)])
            io = IOBuffer()
            Arrow.write(io, (structs=rows,); file=false)
            bytes = take!(io)
            field = Arrow.readstream(bytes).schema.fields[1]
            @test field.type isa Arrow.AC.StructType
            @test isempty(field.children)
            @test Arrow.Table(bytes).structs == [Pair{String,Any}[] for _ in rows]
        end
    end

    @testset "one deterministic PR suite" begin
        summary = SeededFuzz.run_pr_suite(; seed=SEED)
        differential = summary.differential
        @test differential.cases == SeededFuzz.PR_CASES
        @test differential.variants == 6 * SeededFuzz.PR_CASES
        @test differential.rewrites == 6 * SeededFuzz.PR_CASES
        @test differential.scanchecks == 54 * SeededFuzz.PR_CASES
        @test differential.layoutchecks == 49
        @test Set([
            :generated_differential,
            :arrowtypes_extension,
            :arrowtypes_struct_extension,
            :map,
            :map_sorted,
            :dense_union,
            :sparse_union,
            :binary,
            :large_binary,
            :utf8_view,
            :binary_view,
            :fixed_size_binary,
            :fixed_size_list,
            :date_units,
            :time_units,
            :timestamp_units,
            :duration_units,
            :run_end_encoded,
            :nullable_struct_parent,
            :retained_rewrite,
            :statistics_pruning,
            :ranged_no_full_fetch,
        ]) ⊆ differential.coverage

        mutations = summary.mutations
        @test mutations.mutations == SeededFuzz.PR_MUTATIONS
        @test sum(values(mutations.counts)) == SeededFuzz.PR_MUTATIONS
        @test mutations.counts[:validation] + mutations.counts[:allocation_limit] > 0
        @test length(mutations.routecounts) == 41
        @test mutations.determinismchecks == SeededFuzz.PR_MUTATIONS
        @test Set(keys(mutations.operationcounts)) ==
              Set((:flip, :set, :delete, :insert, :truncate))
        @test SeededFuzz.case_fingerprint(SEED, 17) == UInt64(0x2253_fcc7_c2ac_cadb)
    end
end

end # module PropertyTests
