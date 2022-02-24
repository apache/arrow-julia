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

  1. Test the revision to be released
  2. Increment version number in `Project.toml`
  3. Prepare RC and vote (detailed later)
  4. Publish (detailed later)

### Prepare RC and vote

Run `dev/release/release_rc.sh` on working copy of `git@github.com:apache/arrow-julia` not your fork:

```console
$ git clone git@github.com:apache/arrow-julia.git
$ dev/release/release_rc.sh ${RC}
(Send a vote email to dev@arrow.apache.org.
 You can use a draft showen by release_rc.sh for the email.)
```

Here is an example to release RC1:

```console
$ dev/release/release_rc.sh 1
```

The argument of `release_rc.sh` is the RC number. If RC1 has a problem, we'll increment the RC number such as RC2, RC3 and so on.

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

### Publish

We need to do the followings to publish a new release:

  * Publish to apache.org
  * Publish to the General Julia package registry

Run `dev/release/release.sh` to publish to apache.org:

```console
$ dev/release/release.sh ${VERSION} ${RC}
```

Here is an example to release 2.2.1 RC1:

```console
$ dev/release/release.sh 2.2.1 1
```

TODO: How to use JuliaTagBot to publish to the General Julia package registry.

### Verify

TODO