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

package com.palantir.atlasdb.config;

import java.util.function.Supplier;

import org.immutables.value.Value;

import com.palantir.conjure.java.api.config.service.UserAgent;

/**
 * Additional parameters for clients to specify when connecting to remote services.
 */
@Value.Immutable
public interface AuxiliaryRemotingParameters {
    UserAgent userAgent();

    boolean shouldLimitPayload();

    /**
     * Whether clients should retry in the event of connection failures.
     * This value may be ignored and assumed to be true for proxies that implement failover.
     */
    boolean shouldRetry();

    /**
     * Whether this client should support operations that block on the server side.
     * In practice, this means that read and idle connection timeouts may be longer.
     *
     * This is set to true by default, to support legacy behaviour.
     */
    @Value.Default
    default boolean shouldSupportBlockingOperations() {
        return true;
    }

    @Value.Default
    default Supplier<RemotingClientConfig> remotingClientConfig() {
        return () -> RemotingClientConfigs.ALWAYS_USE_CONJURE;
    }

    static ImmutableAuxiliaryRemotingParameters.Builder builder() {
        return ImmutableAuxiliaryRemotingParameters.builder();
    }
}
