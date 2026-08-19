```@raw html
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
```

# API Reference

## Read

```@docs
Arrow.Table
Arrow.Stream
Arrow.close!(::Arrow.Table)
```

## Write

```@docs
Arrow.write
Arrow.DictEncode
```

## Byte-range reads

```@docs
Arrow.AbstractArrowSource
Arrow.sourcelength
Arrow.readrange
Arrow.readranges
```

## The C data and C stream interfaces

```@docs
Arrow.to_c_data
Arrow.from_c_data
Arrow.export_stream!
Arrow.from_c_stream
Arrow.ImportedStream
Arrow.nextbatch!
Arrow.release!(::Arrow.ForeignOwner)
Arrow.release!(::Arrow.ImportedStream)
Arrow.close!(::Arrow.ForeignOwner)
Arrow.reap!
```

## Errors

```@docs
Arrow.ValidationError
```
