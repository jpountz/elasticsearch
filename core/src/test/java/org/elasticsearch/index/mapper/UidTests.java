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
package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.Version;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;

public class UidTests extends ESTestCase {

    public void testDetectUrlSafeBase64() {
        assertTrue(Uid.isUrlSafeBase64(""));
        for (int i = 0; i < 1000; ++i) {
            doTestDetectUrlSafeBase64();
        }

        // need multiples of 4 chars
        assertFalse(Uid.isUrlSafeBase64("A"));
        assertFalse(Uid.isUrlSafeBase64("AA"));
        assertFalse(Uid.isUrlSafeBase64("AAA"));
        assertTrue(Uid.isUrlSafeBase64("AAAA"));
        assertFalse(Uid.isUrlSafeBase64("AAAAA"));

        // too many padding bytes
        assertFalse(Uid.isUrlSafeBase64("A==="));
    }

    private void doTestDetectUrlSafeBase64() {
        String uid = UUIDs.base64UUID();
        assertTrue(Uid.isUrlSafeBase64(uid));
    }

    public void testToIndexTerm() throws Exception {
        assertEquals(new BytesRef("type#id"), new Uid("type", "id").toIndexTerm(Version.V_5_0_0));
        assertEquals(new BytesRef("type#id"), new Uid("type", "id").toIndexTerm(Version.V_2_0_0));

        String autoUid = UUIDs.base64UUID();
        byte[] autoUidBytes = Base64.decode(autoUid, Base64.URL_SAFE);
        byte[] indexUidBytes = new byte[autoUidBytes.length + 5];
        System.arraycopy(autoUidBytes, 0, indexUidBytes, 5, autoUidBytes.length);
        UnicodeUtil.UTF16toUTF8("type", 0, 4, indexUidBytes);
        assertEquals(new BytesRef(indexUidBytes), new Uid("type", autoUid).toIndexTerm(Version.V_5_0_0));
        assertEquals(new BytesRef("type#" + autoUid), new Uid("type", autoUid).toIndexTerm(Version.V_2_0_0));
    }

}
