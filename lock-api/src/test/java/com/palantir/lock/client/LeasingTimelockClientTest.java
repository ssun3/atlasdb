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

package com.palantir.lock.client;


import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.IdentifiedTime;
import com.palantir.lock.v2.LeasableLockResponse;
import com.palantir.lock.v2.LeasableRefreshLockResponse;
import com.palantir.lock.v2.LeasableStartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.TimestampAndPartition;

@RunWith(MockitoJUnitRunner.class)
public class LeasingTimelockClientTest {
    @Mock private TimelockRpcClient timelockService;
    @Mock private LockRequest lockRequest;
    @Mock private StartIdentifiedAtlasDbTransactionRequest startTxnRequest;
    @Mock private TimestampAndPartition timestampAndPartition;

    private TimelockService timelockClient;
    private static final UUID LEADER_ID = UUID.randomUUID();

    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());
    private static final LockResponse LOCK_RESPONSE = LockResponse.successful(LOCK_TOKEN);
    private static final LockResponse FAILED_LOCK_RESPONSE = LockResponse.timedOut();

    @Before
    public void before() {
        when(timelockService.getLeaderTime()).thenReturn(IdentifiedTime.of(UUID.randomUUID(), NanoTime.now()));
        timelockClient = LeasingTimelockClient.create(timelockService);
    }

    @Test
    public void delegatesLockRequest() {
        when(timelockService.lockV2(lockRequest)).thenReturn(
                LeasableLockResponse.of(LOCK_RESPONSE, getLease()));

        timelockClient.lock(lockRequest);

    }

    @Test
    public void lockResponeHasCorrectLeasedLock() {
        Lease lease = getLease();
        when(timelockService.lockV2(lockRequest)).thenReturn(
                LeasableLockResponse.of(LOCK_RESPONSE, lease));

        LockResponse clientResponse = timelockClient.lock(lockRequest);

        verify(timelockService).lockV2(lockRequest);
        LeasedLockToken leasedLockToken = (LeasedLockToken) clientResponse.getToken();
        assertThat(leasedLockToken.serverToken()).isEqualTo(LOCK_TOKEN);
        assertThat(leasedLockToken.getLease()).isEqualTo(lease);
    }

    @Test
    public void shouldHandleUnsuccessfulLockResponses() {
        Lease lease = getLease();
        when(timelockService.lockV2(lockRequest)).thenReturn(
                LeasableLockResponse.of(FAILED_LOCK_RESPONSE, lease));

        LockResponse clientResponse = timelockClient.lock(lockRequest);
        assertFalse(clientResponse.wasSuccessful());
    }

    @Test
    public void startAtlasdbTransactionResponseHasCorrectLeasedLock() {
        StartIdentifiedAtlasDbTransactionResponse response = startTransactionResponseWith(LOCK_TOKEN);
        Lease lease = getLease();
        when(timelockService.startAtlasDbTransactionV3(startTxnRequest)).thenReturn(
                LeasableStartIdentifiedAtlasDbTransactionResponse.of(response, lease));

        StartIdentifiedAtlasDbTransactionResponse clientResponse =
                timelockClient.startIdentifiedAtlasDbTransaction(startTxnRequest);

        verify(timelockService).startAtlasDbTransactionV3(startTxnRequest);

        LeasedLockToken leasedLock = (LeasedLockToken) clientResponse.immutableTimestamp().getLock();
        assertThat(leasedLock.serverToken()).isEqualTo(LOCK_TOKEN);
        assertThat(leasedLock.getLease()).isEqualTo(lease);
    }

    @Test
    public void returnedTokenShouldHaveCorrectServerToken() {
        when(timelockService.lockV2(lockRequest)).thenReturn(
                LeasableLockResponse.of(LOCK_RESPONSE, getLease()));

        LockResponse lockResponse = timelockClient.lock(lockRequest);
        LeasedLockToken leasedToken = (LeasedLockToken) lockResponse.getToken();
        assertThat(LOCK_RESPONSE.getToken()).isEqualTo(leasedToken.serverToken());
    }

    @Test
    public void leasedTokenShouldHaveValidLeaseForTheLeasePeriod() {
        when(timelockService.lockV2(lockRequest)).thenReturn(
                LeasableLockResponse.of(LOCK_RESPONSE, getLease()));

        LockResponse lockResponse = timelockClient.lock(lockRequest);
        assertValid(lockResponse.getToken());
    }

    @Test
    public void unlockShouldCallRemoteServer_validLeases() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        assertValid(token);
        timelockClient.unlock(ImmutableSet.of(token));

        verify(timelockService).unlock(ImmutableSet.of(token.serverToken()));
    }

    @Test
    public void unlockShouldCallRemoteServer_inValidLeases() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease(Duration.ZERO));
        assertInvalid(token);
        timelockClient.unlock(ImmutableSet.of(token));

        verify(timelockService).unlock(ImmutableSet.of(token.serverToken()));
    }

    @Test
    public void unlockShouldInvalidateLease() {
        LockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        timelockClient.unlock(ImmutableSet.of(token));

        assertInvalid(token);
    }

    @Test
    public void shouldOnlyCallIdentifiedTimeIfLeaseIsValid() {
        LeasedLockToken validToken = LeasedLockToken.of(LOCK_TOKEN, getLease());
        when(timelockService.getLeaderTime()).thenReturn(getIdentifiedTime());
        timelockClient.refreshLockLeases(ImmutableSet.of(validToken));

        verify(timelockService).getLeaderTime();
        verifyNoMoreInteractions(timelockService);
    }

    @Test
    public void shouldRefreshTheLease_invalidOnClient_validOnServer() {
        LeasedLockToken leasedLockToken = LeasedLockToken.of(LOCK_TOKEN, getLease(Duration.ZERO));
        assertInvalid(leasedLockToken);

        when(timelockService.refreshLockLeasesV2(ImmutableSet.of(LOCK_TOKEN)))
                .thenReturn(LeasableRefreshLockResponse.of(
                        ImmutableSet.of(LOCK_TOKEN),
                        getLease()
                ));

        Set<LockToken> refreshed = timelockClient.refreshLockLeases(ImmutableSet.of(leasedLockToken));
        verify(timelockService).refreshLockLeasesV2(ImmutableSet.of(leasedLockToken.serverToken()));

        LeasedLockToken refreshedLeasedLockToken = (LeasedLockToken) refreshed.iterator().next();
        assertValid(refreshedLeasedLockToken);
        assertValid(leasedLockToken);
        assertThat(refreshedLeasedLockToken).isEqualTo(leasedLockToken);
    }

    private StartIdentifiedAtlasDbTransactionResponse startTransactionResponseWith(LockToken lockToken) {
        return StartIdentifiedAtlasDbTransactionResponse.of(
                LockImmutableTimestampResponse.of(1L, lockToken),
                timestampAndPartition
        );
    }

    private void assertValid(LockToken token) {
        LeasedLockToken leasedLockToken = (LeasedLockToken) token;
        assertTrue(leasedLockToken.isValid(getIdentifiedTime()));
    }

    private void assertInvalid(LockToken token) {
        LeasedLockToken leasedLockToken = (LeasedLockToken) token;
        assertFalse(leasedLockToken.isValid(getIdentifiedTime()));
    }

    private Lease getLease(Duration period) {
        return Lease.of(getIdentifiedTime(), period);
    }

    private Lease getLease() {
        return Lease.of(getIdentifiedTime(), Duration.ofSeconds(1L));
    }

    private IdentifiedTime getIdentifiedTime() {
        return IdentifiedTime.of(LEADER_ID, NanoTime.now());
    }
}
