# Arrow Review

1. Question
have we done an in-depth review/analysis of the vendored FlatBuffers code? it was originally ported from the Go implementation. we should do a pass to identify bugs, perf issues, and functionality gaps. it probably wouldn't take much to get it up to a state where we could actually upstream to the FlatBuffers.jl package. we should also compare w/ the actual current FlatBuffers.jl implementation and figure out the current state of everything and make a plan forward (kinda dumb to keep vendoring FB in Arrow). Like, maybe our fbsgen.jl can be improved/generalized and used upstream?

2. Question
"Canonical padding and unused-bit checks remain production work." uh....what does that mean? is there work to do for this in the core module?

3. Follow up
"""
# What this deliberately gives up, so the constraint is informed:
#   * Eager, deterministic unmap (e.g. delete-a-mapped-file-now on Windows):
#     unmapping happens when the GC collects the mapping. Revisit if real
#     demand appears, likely as an opt-in layer once upstream offers a
#     public API.
"""
I actually think we can still support this via explicit API. i.e I think it would be reasonable to provide an `Arrow.close(::Arrow.Table)` like API, or maybe call it `Arrow.close!` to semantically distinguish from the traditional Base IO-related `close`. The idea is it would do a force munmap on underlying array storage (i.e. calling `finalize(buf)`) so we could still deterministically say a file was done being used for windows. We could carry a simple `@atomic closed::Bool` field on OwnerRegion and do a check on loadat! to make things safe and prevent use-after-release. That seems like a *much* simpler plan than was originally proposed; mine is one public api entrypoint + one atomic bool field + a few checks. In Arrow.Table's docs we then throw a little blurb near the end about calling this `close!` on windows for deterministic release of data files.

4. Clean up
We have a ton of code comments + docs that mention the rewrite process, decisions made/not made/deferred, how stuff *used* to be, etc. We need to clean all that up. Code comments + docs should always reflect the current state of things, nothing more/less. For historical context, we can look at git history and maybe I'll do a blog post write up or give a JuliaCon talk on the whys, how, where we came from.

5. Follow up
I think we could rework the _value and _value_of functions to allow passing a desired element type down and basically asserting that we get that back out. For the most part, it maps already to teh ArrowType for primitives (i.e IntType & Int64). But I think it will be a key piece of actually allowing a useable trim-compile workload; i.e. I can provide you with a static schema for the columns (names & types) I know i'm operating on: arrow materializes using the core C-data structures, then the entrypoints allow full static resolvability as I pass my schema column types down through _value. The trickier bit there will probably be custom structs, maybe Unions and other aggregates/complex types, but we do also already have StructUtils and its machinery and code to either use directly (overload applyeach?) or copy the patterns of. We can certainly still pass `Any` down as the default struct type and do the Vector{Pair{String,Any}} thing, I just think we should *also* _allow_ doing our normal trim-compile tricks, i.e. passing known/fixed schemas/types down to get static resolution.

6. Clean up
"""
_malloc!(root::ExportedRoot, n::Integer,
    register! = push!, deallocate! = Libc.free) = begin
"""
Big no-no; we don't want to ever use the short-form method definition form (w/ =) with a begin-end block.

7. Question
There have been a couple C-data attempts in Arrow.jl over the years; none were really that serious from waht I remember, but
just recently some kind put up a few PRs. I haven't had a chance to look at them, but I feel a little bad if he put a ton of time into them just have me throw everything away. We should compare what he has and at least see if there are parts we can incorporate or overall APIs we should use.

8. Follow up
We should make sure the C-data entrypoints are explicitly trim-compile-able (I mean, they're C, so they *should be*). I can see that being a killer use-case: I'm going to trim-compile this super fast binary that gets data into Julia, does some munging/calculating, then spits it back out somewhere using C-data at the seams.

9. Follow up
Let's remove all the demo stuff at the end of the cdata.jl file

10. Follow up
"experimental V4 IPC compression is outside this prove-out" what ist he experimental compression? how hard to support? or is it actually old and we *don't* want to support it?

11. Clean up
get rid of _threaded_cursor_stress in ipc_read.jl

12. Follow up
I do really like the design of ArrowCore with C-data & IPC as layers on top. we're doing *a lot* more work though, *it seems*, for serialize/deserialize. I know it's all justified because we want to do all the validation/verification stuff and we're a lot safer now, but I do worry about the performance cost of it all. At the end of the day, arrow data is supposed to be *fast*. Can we do some benchmarking on arrow rewrite vs. Arrow.jl 2.X and then pick, what, pyarrow as probably the industry-best impl? I want to make sure we're *fast* and don't get laughed at as soon as we release 3.0.

13. Question
Why do we need _resolvefilter in scan.jl? I thought Tables.bind basically handled that?

14. Question
in scan.jl, we have `return table, Tables.Scan(residualselect, residualfilter, limit, offset, scan.validate)`;
what are the cases where we have residuals of any of these (select, filter, limit, offset, etc.)? it feels like we should
be able to handle all of them? and never have residuals?

15. Question
in scan.jl, we have statistics data/batch/meta stuff? what's that all about? is it in the actual arrow spec? but at least one of our comments mentioned having our own stats spec or something?

16. Follow up
_statcmp and _stateq in scan.jl need to be full function defs, or actually one-line method defs.

17. Follow up
Ok, finished reviewing all the Arrow rewrite; it's in pretty good shape! I didn't realize we hadn't done the facade layer yet though. So that's the next request: let's build out the Arrow.Table, Arrow.Stream, Arrow.write, etc. layers on top of what we have in ipc_read.jl/ipc_write.jl. We should make sure to allow to pass a Tables.Scan to Arrow.Table and that will utilize the new scan pushdown. We also need to figure out a good interface for getting the RangedFile stuff exposed; brainstorm some ideas here. If there are awkward parts of the public surface area that don't make sense in teh rewrite or aren't supported anymore, or led to bad behavior, I'm open to changing/breaking things. I think if it's justified, we can help people adjust to whatever is better. If we can add simple compat things though, that's always nice.

18. Follow up
In parallel, we've been working on rewrites for both Arrow & CSV. One of the pieces in CSV is we have a new CompactString type that actually should match the ArrowCore string representation. I want to make sure we can "automatically" get zero-copy Arrow.Table when reading from CSV and have it all "just work".

19. Follow up
How is our overall round-tripping testing vs. other frameworks? extensive? breadth & depth? stream/file/c-data/ipc/all the things? I want those cross-impl tests to be the meat of our testing. yes we can still have some unit tests on other things, and a few facade/interface tests, but the meat should be exhaustive round-tripping with like pyarrow and nanoarrow.