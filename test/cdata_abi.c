/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE
struct ArrowSchema {
    const char *format;
    const char *name;
    const char *metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema **children;
    struct ArrowSchema *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void **buffers;
    struct ArrowArray **children;
    struct ArrowArray *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};
#endif

#define LAYOUT(T) \
    printf(#T ".size=%zu\n", sizeof(struct T)); \
    printf(#T ".alignment=%zu\n", _Alignof(struct T))
#define FIELD(T, F) printf(#T "." #F "=%zu\n", offsetof(struct T, F))

int main(void) {
    printf("pointer.size=%zu\n", sizeof(void *));
    printf("int64.alignment=%zu\n", _Alignof(int64_t));
    LAYOUT(ArrowSchema);
    FIELD(ArrowSchema, format);
    FIELD(ArrowSchema, name);
    FIELD(ArrowSchema, metadata);
    FIELD(ArrowSchema, flags);
    FIELD(ArrowSchema, n_children);
    FIELD(ArrowSchema, children);
    FIELD(ArrowSchema, dictionary);
    FIELD(ArrowSchema, release);
    FIELD(ArrowSchema, private_data);
    LAYOUT(ArrowArray);
    FIELD(ArrowArray, length);
    FIELD(ArrowArray, null_count);
    FIELD(ArrowArray, offset);
    FIELD(ArrowArray, n_buffers);
    FIELD(ArrowArray, n_children);
    FIELD(ArrowArray, buffers);
    FIELD(ArrowArray, children);
    FIELD(ArrowArray, dictionary);
    FIELD(ArrowArray, release);
    FIELD(ArrowArray, private_data);
    return 0;
}
