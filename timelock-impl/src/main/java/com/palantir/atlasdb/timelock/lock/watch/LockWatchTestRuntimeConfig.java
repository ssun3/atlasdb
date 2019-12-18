/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.lock.watch;

import java.util.Optional;
import java.util.Set;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.TableReference;

@JsonSerialize(as = ImmutableLockWatchTestRuntimeConfig.class)
@JsonDeserialize(as = ImmutableLockWatchTestRuntimeConfig.class)
@Value.Immutable
public abstract class LockWatchTestRuntimeConfig {
    @Value.Default
    @JsonProperty("namespace")
    Optional<String> namespaceToWatch() {
        return Optional.empty();
    }

    @Value.Default
    @JsonProperty("tables")
    public Set<TableReference> tablesToWatch() {
        return ImmutableSet.of();
    }

    public static LockWatchTestRuntimeConfig defaultConfig() {
        return ImmutableLockWatchTestRuntimeConfig.builder().build();
    }
}
