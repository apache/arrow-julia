<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
--->

# Nested public values

## The rule

The public value domain is uniform at every depth. A facade-converted leaf
— Date32/Date64, Time, Timestamp (zone-naive and zone-declared), Duration,
supported Decimal, Interval — materializes as the same public Julia value
whether it is a top-level column, a dictionary pool, a run-end-encoded
values child, a union branch, a struct field, or a list/map element. The
same holds in reverse: the writer accepts those public values at every
depth, fresh and retained.

This replaces the pre-3.0-release rule where only the top level and
dictionary encoding converted, and nested temporal leaves stayed raw
storage integers inside `Vector{Pair{String,Any}}` / `Vector{Any}` rows
(the `converted::Bool` threading through `_declaredeltype`, and
`_rawdeclaredbasetype`). That asymmetry — and the write-side refusal of
nested public values that mirrored it — is removed.

## Layering

* **ArrowCore stays the storage authority, untouched.** `AC._value`,
  `AC.materialize`, and `AC.fromjulia` keep the storage domain: raw counts
  for temporals, raw coefficients for decimals. They serve the C interface,
  conformance, and the documented low-level `Arrow.materialize` API. Core
  keeps zero knowledge of Dates/Durations/DataDecimals public types and
  remains trim-compileable on its own.

* **One leaf-conversion authority in the facade.** A single mapping per
  convertible leaf descriptor supplies the triple (public element type,
  storage→public scalar, public→storage scalar). `_facadebasetype` (types),
  `_publicleafvalue` (storage→public; the per-element body `_postconvert`
  loops), and `_exactfacadescalar`/`_facadetoken` (public→storage) are that
  authority. Every reader, writer, and scan-planner rule reads it; no
  second copy of any conversion exists.

* **The facade owns a recursive public row builder.** For composite and
  wrapper roots the facade builds rows itself (`_publicvalue`), mirroring
  Core's `_value` shapes — `Vector{Pair{String,Any}}` struct rows,
  `Vector{Any}` list rows, `Pair{Any,Any}` map rows — but converting leaf
  values through the authority AS the row is built. Union branches convert
  at build time, while the type id is in hand (a built row cannot recover
  its branch, so post-hoc conversion is impossible; this is why conversion
  moved into row construction). Top-level scalar columns keep the typed
  bulk path plus one whole-column conversion pass; run-end-encoded and
  dictionary roots keep their flat expansion plus one whole-column pass
  over the values domain (the expansion output is governed by one
  descriptor, so the one-pass rewrite stays valid and fast).

* **Declared types have no `converted` flag.** `_declaredeltype(f)` walks
  the field tree and every leaf answers with its public type. Empty,
  all-missing, and populated columns of one field agree by construction.

* **The writer is symmetric.** Fresh composite columns accept public leaf
  values (a `NamedTuple` field of `DateTime` or `Timestamp{P}` lowers
  through the authority instead of refusing), and retained write-back
  lowers nested public values through the same public-to-storage authority
  the top level uses (`_retainedstorage`/`_facadetostorage` over
  `_exactfacadevalue`). A value that cannot lower exactly refuses,
  exactly as at the top level.

* **ArrowTypes-labeled subtrees are a different domain on purpose.** An
  extension-labeled field routes through the ArrowTypes contract
  (`ArrowType(T)`-declared storage views); registered mappings written
  against that interface keep working unchanged. The boundary is the
  extension label, as before.

* **The allocation budget covers the dynamic rows' overhead, not just their
  payload.** The per-row builders run dynamically typed, so each value
  leaves transient boxes beside the payload Core's budgeted `getvalue`
  reserves. The facade charges a fixed measured-worst-case reserve at each
  seam: per converted leaf (facade and raw-domain restoration), per
  shared-decimal `_postconvert` element (the one type-unstable `_mapcol`
  body — the stable bodies charge nothing extra), per routed-walker leaf
  (Core's transient estimate object), per value lifted through a
  registered `fromarrow` hook (fixed reserves split three ways: childless
  scalar storage, width-charged container storage, and union or
  transparent-wrapper storage), per struct-hook child (the `applicable`
  probe and the variadic hook call re-splat and re-box every child
  value) plus the row's inline payload size (reconstruction copies isbits
  storage wholesale at each seam, so struct charges scale with
  `sizeof` of the constructed row — heap children count as pointer
  slots), per lifted string's byte length (the default ArrowTypes pointer
  adapter copies it), and per nested restoration call
  (`_chargerestoredelements!`, the dynamic keyword-call tuple, with an
  extra share on the raw-domain struct tail). The other
  half of the model is staying allocation-free where no reserve exists:
  null slots return `missing` before the budgeted extraction (NullType
  first — its layout has no validity buffer), every per-value cache
  lookup (`_arrowtypestarget`, `_hasarrowtypesextension`, the eltype,
  dictionary-field and storage-type memos) returns hits without
  constructing the memoization thunk, the wide-decimal scalar conversion
  runs unboxed behind `_shareddecimalscalar`'s type barrier, restoration
  loops hand child Fields to dynamic calls through the per-Field
  `_arrowtypesboxedchildren` cache instead of re-boxing them per row, the
  struct and map branches read their storage base type from the target
  and eltype caches instead of rebuilding it per row (an unknown label
  keeps the direct computation: its cached slot holds the bounded
  fallback shape), and the row-lifting loop boxes its `Field` once behind
  `_boxonce`, which the optimizer cannot fold away. The reserves were
  sized by measuring
  flat/union/REE/struct/list roots × timestamp/decimal/date leaves ×
  unlabeled/unknown-label/registered-label shapes; a nestedvalues testset
  pins per-value charge floors for each seam and (on current Julia) that a
  warmed read allocates no more than it charges.

## Trim contract

Two tiers, by what the caller knows statically:

* **Dynamic schema** (runtime-discovered): the recursive builder is
  dynamic dispatch over `Any`-shaped rows and is NOT trim-guaranteed.
* **Static schema**: two surfaces. Core's typed `getvalue(::Type{T}, ...)`
  and `materialize(::Type{T}, ...)` claims stay the storage-domain
  monomorphic tier and remain part of the trim workload, unchanged. The
  facade's typed select override (`:col => NamedTuple{...}`, recursively,
  including `Vector{...}` targets) now accepts PUBLIC leaf types such as
  `Durations.Timestamp{Nanosecond}` — the public rows already hold those
  values, so the override is an identity check per leaf rather than a
  conversion. The override tier is a convenience over the dynamic rows and
  is not trim-audited; making the whole facade trim-compileable stays
  future work.

The typed tiers share no code with the dynamic row shapes, so the dynamic
`Pair{String,Any}` containers cost them nothing.

## Scan and statistics

Filter literals lower per-FIELD through the same authority. A composite
column still evaluates its filters in the public domain (no composite
pushdown), which is now also the converted domain — plain Julia `==` over
public rows is the semantics. `_nativefacadeconversion`'s children test
keeps composite fields off the storage plan. Statistics pruning is
top-level and unchanged.
