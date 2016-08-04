/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

/** These are essentially flake ids (http://boundary.com/blog/2012/01/12/flake-a-decentralized-k-ordered-unique-id-generator-in-erlang) but
 *  we use 6 (not 8) bytes for timestamp, and use 3 (not 2) bytes for sequence number. */

class TimeBasedUUIDGenerator implements UUIDGenerator {

    // We only use bottom 3 bytes for the sequence number.  Paranoia: init with random int so that if JVM/OS/machine goes down, clock slips
    // backwards, and JVM comes back up, we are less likely to be on the same sequenceNumber at the same time:
    private final AtomicInteger sequenceNumber = new AtomicInteger(SecureRandomHolder.INSTANCE.nextInt());

    // Used to ensure clock moves forward:
    private long lastTimestamp;

    private static final byte[] SECURE_MUNGED_ADDRESS = MacAddressProvider.getSecureMungedAddress();

    static {
        assert SECURE_MUNGED_ADDRESS.length == 6;
    }

    @Override
    public String getBase64UUID()  {
        final int sequenceId = sequenceNumber.incrementAndGet() & 0xffffff;
        long timestamp = System.currentTimeMillis();

        synchronized (this) {
            // Don't let timestamp go backwards, at least "on our watch" (while this JVM is running).  We are still vulnerable if we are
            // shut down, clock goes backwards, and we restart... for this we randomize the sequenceNumber on init to decrease chance of
            // collision:
            timestamp = Math.max(lastTimestamp, timestamp);

            if (sequenceId == 0) {
                // Always force the clock to increment whenever sequence number is 0, in case we have a long time-slip backwards:
                timestamp++;
            }

            lastTimestamp = timestamp;
        }

        final byte[] uuidBytes = new byte[15];
        int i = 0;

        // We use the lower 6 bytes of the timestamp in the uid and try to put
        // the most discriminant ones closer to the beginning
        uuidBytes[i++] = (byte) (timestamp >>> 8);
        uuidBytes[i++] = (byte) (timestamp >>> 16);
        uuidBytes[i++] = (byte) (timestamp >>> 24);
        uuidBytes[i++] = (byte) (timestamp >>> 32);
        uuidBytes[i++] = (byte) (timestamp >>> 40);

        // MAC address adds 6 bytes:
        System.arraycopy(SECURE_MUNGED_ADDRESS, 0, uuidBytes, 5, SECURE_MUNGED_ADDRESS.length);
        i += SECURE_MUNGED_ADDRESS.length;

        // Now add the lowest byte of the timestamp and the counter, they are less discriminant
        // (they could appear in any segment) so we put them last
        uuidBytes[i++] = (byte) (sequenceId >>> 16);
        uuidBytes[i++] = (byte) (sequenceId >>> 8);
        uuidBytes[i++] = (byte) sequenceId;
        uuidBytes[i++] = (byte) timestamp;

        assert i == uuidBytes.length;

        return Base64.getUrlEncoder().withoutPadding().encodeToString(uuidBytes);
    }
}
