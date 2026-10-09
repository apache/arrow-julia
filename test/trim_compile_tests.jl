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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# JuliaC `--trim=safe` compile gate for ArrowCore and DataStrings, following the harness
# convention from JSON/HTTP/Reseau/StructUtils: compile the workload
# entrypoint, require ZERO verifier errors and ZERO verifier warnings, then
# run the produced binary and require exit 0.
#
# Run explicitly (needs network on first run to install JuliaC):
#     julia --startup-file=no test/trim_compile_tests.jl
#
# It is intentionally NOT included by test/runtests.jl (it installs JuliaC
# and compiles a binary; the default suite stays fast).

using Test
import Pkg

const _TRIM_SUPPORTED = VERSION >= v"1.12.0-rc1"
const _JULIAC_ENTRYPOINT_EXPR = "using JuliaC; if isdefined(JuliaC, :main); JuliaC.main(ARGS); else JuliaC._main_cli(ARGS); end"
const _TRIM_COMPILE_TIMEOUT_S = 600.0
const _TRIM_RUN_TIMEOUT_S = 60.0

function _prepare_trim_project(trim_project::String)::Nothing
    mkpath(trim_project)
    cp(
        joinpath(@__DIR__, "trim", "Project.toml"),
        joinpath(trim_project, "Project.toml");
        force=true,
    )
    original_project = Base.active_project()
    try
        Pkg.activate(trim_project)
        Pkg.instantiate()
    finally
        original_project === nothing || Pkg.activate(dirname(original_project))
    end
    return nothing
end

function _run_with_timeout(cmd::Cmd; timeout_s::Float64, label::String)
    output_path = tempname()
    out = open(output_path, "w")
    exit_code = -1
    timed_out = false
    try
        proc = run(pipeline(ignorestatus(cmd), stdout=out, stderr=out); wait=false)
        started = time()
        next_log = started + 15.0
        while Base.process_running(proc)
            if time() - started >= timeout_s
                kill(proc)
                timed_out = true
                break
            end
            if time() >= next_log
                println("[trim] $(label) WAIT $(round(time() - started; digits=1))s")
                flush(stdout)
                next_log = time() + 15.0
            end
            sleep(0.2)
        end
        timed_out || wait(proc)
        exit_code = something(proc.exitcode, -1)
    finally
        close(out)
    end
    output = try
        read(output_path, String)
    catch
        ""
    finally
        rm(output_path; force=true)
    end
    return exit_code, output, timed_out
end

function _count_verifier_messages(output::String)::Tuple{Int,Int}
    errors = length(collect(eachmatch(r"Verifier error #\d+:", output)))
    warnings = length(collect(eachmatch(r"Verifier warning #\d+:", output)))
    return errors, warnings
end

@testset "ArrowCore and DataStrings trim compile" begin
    if !_TRIM_SUPPORTED
        println("[trim] skip: JuliaC --trim requires Julia >= 1.12")
    elseif Sys.iswindows()
        println("[trim] skip Windows: JuliaC trim compilation stalls on Windows CI")
    else
        script_path = joinpath(@__DIR__, "trim_entrypoint.jl")
        @test isfile(script_path)
        mktempdir() do tmp
            trim_project = joinpath(tmp, "trimproj")
            _prepare_trim_project(trim_project)
            cd(tmp) do
                julia_exe = joinpath(Sys.BINDIR, Base.julia_exename())
                cmd = `$julia_exe --startup-file=no --history-file=no
                    --project=$trim_project -e $_JULIAC_ENTRYPOINT_EXPR --
                    --output-exe arrow_trim --project=$trim_project
                    --experimental --trim=safe $script_path`
                println("[trim] compile START")
                exit_code, output, timed_out = _run_with_timeout(
                    cmd;
                    timeout_s=_TRIM_COMPILE_TIMEOUT_S,
                    label="compile",
                )
                timed_out && error("trim compile timed out\n$output")
                errors, warnings = _count_verifier_messages(output)
                if errors > 0 || warnings > 0 || exit_code != 0
                    println("---- trim compile output ----")
                    println(output)
                    println("---- end output ----")
                end
                @test errors == 0
                @test warnings == 0
                @test exit_code == 0
                binpath = abspath("arrow_trim")
                @test isfile(binpath)
                run_exit, run_output, run_timed_out = _run_with_timeout(
                    `$binpath`;
                    timeout_s=_TRIM_RUN_TIMEOUT_S,
                    label="run",
                )
                run_timed_out && error("trim executable timed out\n$run_output")
                if run_exit != 0
                    println("---- trim executable output ----")
                    println(run_output)
                    println("---- end output ----")
                end
                @test run_exit == 0
                println("[trim] compile + run PASSED")
            end
        end
    end
end
