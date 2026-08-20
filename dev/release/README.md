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
-->

# Release

## Overview

  1. Wait for a registered Tables.jl release that contains `Tables.Scan`.
  2. Remove the temporary Tables.jl branch override, set the exact minimum
     Tables.jl version, and test the revision to be released from a clean
     environment.
  3. Set each package being released to its final version. Do not use a
     prerelease suffix such as `-DEV`.
  4. Prepare the signed source RC and run the Apache vote.
  5. After the vote passes, publish the approved Apache source release.
  6. Register ArrowTypes.jl and ArrowStrings.jl. These registrations can start
     together. Wait for the ArrowStrings.jl General registry PR to merge.
  7. Register Arrow.jl 3.0 with breaking-change release notes.

ArrowStrings.jl is part of this ASF source tree. Do not register it in General
before the PMC approves the source RC. General registration makes the package
available outside the Apache Arrow development community and is therefore a
release action under ASF policy.

### Prepare RC and vote

Before making an RC, confirm all of the following:

  * Tables.jl with `Tables.Scan` is released in General.
  * Arrow.jl resolves against that registered Tables.jl version without a
    `[sources]` entry, a commit URL, or a developed checkout. Remove the
    temporary Tables setup from CI, documentation, and the conformance image.
  * `Project.toml` contains the final release version, such as `3.0.0`, and not
    `3.0.0-DEV`.
  * ArrowStrings.jl has its final `1.0.0` package version in
    `src/ArrowStrings/Project.toml`.
  * Every subpackage that will be distributed has its own complete
    `LICENSE.md` and `NOTICE` files. A General subdirectory package does not
    include the repository-root copies.
  * ArrowTypes.jl has its final package version. This RC includes ArrowTypes.jl
    2.4.0, which follows the registered 2.3.0 release.
  * ArrowStrings.jl passes General/RegistryCI preflight checks for its name,
    license, installation, loading, dependencies, and compatibility bounds.
    Fix any source problem before the vote; an approved source archive cannot
    be changed in place.
  * Every required CI job and the full conformance workflow passed on the exact
    commit proposed for the RC. Use a manual conformance dispatch if the commit
    did not run through a pull request.
  * The working tree is clean and the release commit is on `main`.

Run `dev/release/release_rc.sh` on working copy of `git@github.com:apache/arrow-julia` not your fork:

```console
$ git clone git@github.com:apache/arrow-julia.git
$ dev/release/release_rc.sh ${RC}
(Send a vote email to dev@arrow.apache.org.
 You can use a draft shown by release_rc.sh for the email.)
```

Here is an example to release RC1:

```console
$ dev/release/release_rc.sh 1
```

The argument of `release_rc.sh` is the RC number. If RC1 has a problem, we'll increment the RC number such as RC2, RC3 and so on.

Keep the vote open for at least 72 hours. A release vote passes only when at
least three PMC members cast binding `+1` votes and there are more positive
than negative binding votes. A shorter vote is only for exceptional expedited
releases and the vote email must explain the reason. See the
[ASF release policy](https://www.apache.org/legal/release-policy.html) and
[ASF voting rules](https://www.apache.org/foundation/voting.html).

After the vote closes, send a `[RESULT][VOTE][Julia]` reply to the vote thread.
State the binding and non-binding totals and whether the vote passed. Do not
publish any package if the vote did not pass.

Requirements to run `release_rc.sh`:

  * You must be an Apache Arrow committer or PMC member
  * You must prepare your PGP key for signing

If you don't have a PGP key, https://infra.apache.org/release-signing.html#generate may be helpful.

Your PGP key must be registered to the followings:

  * https://dist.apache.org/repos/dist/dev/arrow/KEYS
  * https://dist.apache.org/repos/dist/release/arrow/KEYS

See the header comment of them how to add a PGP key.

Apache arrow committers can update them by Subversion client with their ASF account. e.g.:

```console
$ svn co https://dist.apache.org/repos/dist/dev/arrow
$ cd arrow
$ editor KEYS
$ svn ci KEYS
```

### Verify

We have a script to verify an RC.

You must install the following commands to use the script:

  * `curl`
  * `gpg`
  * `shasum` or `sha256sum`/`sha512sum`
  * `tar`

You do not need to install Julia. If Julia is not installed, the script
downloads the latest release only for verification.

To verify an RC, run:

```console
$ dev/release/verify_rc.sh ${VERSION} ${RC}
```

For example:

```console
$ dev/release/verify_rc.sh 3.0.0 1
```

The script prints `RC looks good!` after it verifies the signature, checksums,
and package tests. A binding `+1` voter must also inspect the source archive for
ASF policy compliance, including its `LICENSE` and `NOTICE`, and verify the
signed source on their own hardware. Run the release audit tool against the
downloaded archive as an additional check:

```console
$ dev/release/run_rat.sh apache-arrow-julia-${VERSION}.tar.gz
```

### Publish

Only continue after the PMC vote passes. We need to do the following to publish
a new release:

  * Publish to apache.org
  * Publish to the Julia General registry

Run `dev/release/release.sh` to publish to apache.org:

```console
$ dev/release/release.sh ${VERSION} ${RC}
```

Here is an example to release 2.2.1 RC1:

```console
$ dev/release/release.sh 2.2.1 1
```

Add the release to ASF's report database via [Apache Committee Report Helper](https://reporter.apache.org/addrelease.html?arrow).

Wait at least one hour after uploading the release before updating download
pages or sending release announcements. This gives the ASF mirrors time to
synchronize.

The Julia General registrations must use the exact commit approved by the PMC.
Register the packages in the order below. The ArrowTypes.jl and ArrowStrings.jl
comments may be posted together because neither registration depends on the
other.

#### 1. Register ArrowTypes.jl if its version changed

Arrow.jl 3.0 does not depend on ArrowTypes.jl. This registration is optional,
but a changed version in `src/ArrowTypes/Project.toml` must either be registered
or reverted before the RC. Post this comment on the approved release commit:

```markdown
@JuliaRegistrator register subdir=src/ArrowTypes
```

#### 2. Register ArrowStrings.jl 1.0.0

Post this comment on the approved release commit:

```markdown
@JuliaRegistrator register subdir=src/ArrowStrings
```

ArrowStrings.jl is a new package. General currently applies a three-day review
period to new packages. Wait for its General PR to merge. Then verify from a
clean environment that `Pkg.add("ArrowStrings")` and `import ArrowStrings`
succeed. Do not trigger the Arrow.jl registration before this step completes.
If a maintainer comments on the General PR, include `[noblock]` where
appropriate so that the maintainer's own comment does not stop AutoMerge.

#### 3. Register Arrow.jl

Post the following on the same approved release commit. Keep the `Release
notes:` heading and the `## Breaking changes` heading. Replace or extend the
bullets so that they match the final migration guide:

```markdown
@JuliaRegistrator register

Release notes:

## Breaking changes

- Arrow.jl 3.0 is a complete implementation rewrite. It returns materialized
  Julia vectors instead of the lazy ArrowVector types used by Arrow.jl 2.x.
- Arrow.jl now requires Julia 1.10 or later.
- `Arrow.write(io, table)` now writes the IPC file format by default. Pass
  `file=false` for the stream format.
- Incremental writing (`Arrow.Writer` and `Arrow.append`), `convert=false`,
  multithreaded encoding, and ArrowTypes.jl custom-type serialization are not
  available in 3.0.
- Read the 3.0 migration guide before updating:
  https://github.com/apache/arrow-julia/blob/main/docs/src/migration.md
```

General requires release notes for a breaking release. A bare
`@JuliaRegistrator register` comment will not qualify for AutoMerge. New
versions of an existing package currently have a 15-minute General review
period. Confirm the current timing and requirements in the
[General registry instructions](https://github.com/JuliaRegistries/General#automatic-merging-of-pull-requests)
before registration.

JuliaRegistrator opens one General PR for each comment. After each PR merges,
the TagBot workflow creates the package tag and GitHub release. The subpackage
tags are `ArrowTypes-vX.Y.Z` and `ArrowStrings-vX.Y.Z`; the root package tag is
`vX.Y.Z`. Confirm that each expected tag points to the approved release commit.
The first monorepo TagBot run may discover older registered ArrowTypes.jl
versions. Run it deliberately before release and inspect any historical tags it
creates rather than discovering the backfill during the release.

### Finish the release

After every General PR and TagBot job completes:

  * Verify each package installs and loads from a clean depot.
  * Verify the root and subpackage tags point to the approved commit and that
    the GitHub releases contain the expected notes.
  * Verify the documentation deployment and the Apache download/release pages.
  * After the mirror delay, send the Apache release announcement.
  * Confirm old RC artifacts were removed and record the release in the Apache
    Committee Report Helper.
  * Bump `main` to the next development version and start the next changelog
    section.

See the broader [Apache Arrow release guide](https://arrow.apache.org/docs/dev/developers/release.html)
for project-wide announcement and publication duties.
