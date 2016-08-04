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
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public final class Uid {

    public static final char DELIMITER = '#';
    public static final byte DELIMITER_BYTE = 0x23;

    private final String type;
    private final String id;
    private final boolean singleType;

    public Uid(String type, String id, boolean singleType) {
        this.type = type;
        this.id = id;
        this.singleType = singleType;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Uid uid = (Uid) o;

        if (id != null ? !id.equals(uid.id) : uid.id != null) return false;
        if (type != null ? !type.equals(uid.type) : uid.type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return createUid(type, id, singleType);
    }

    public BytesRef toBytesRef() {
        return createUidAsBytes(type, id, singleType);
    }

    /**
     * @param uid the uid, as stored in the index
     * @param singleType the single type in the index only has a single type, or null otherwise
     */
    public static Uid createUid(String uid, String singleType) {
        if (singleType != null) {
            return new Uid(singleType, uid, true);
        } else {
            int delimiterIndex = uid.indexOf(DELIMITER); // type is not allowed to have # in it..., ids can
            return new Uid(uid.substring(0, delimiterIndex), uid.substring(delimiterIndex + 1), false);
        }
    }

    public static BytesRef createUidAsBytes(String type, String id, boolean singleType) {
        if (singleType) {
            return new BytesRef(id);
        } else {
            return createUidAsBytes(new BytesRef(type), new BytesRef(id), singleType);
        }
    }

    public static BytesRef createUidAsBytes(String type, BytesRef id, boolean singleType) {
        if (singleType) {
            return id;
        } else {
            return createUidAsBytes(new BytesRef(type), id, singleType);
        }
    }

    public static BytesRef createUidAsBytes(BytesRef type, BytesRef id, boolean singleType) {
        if (singleType) {
            return id;
        } else {
            final BytesRef ref = new BytesRef(type.length + 1 + id.length);
            System.arraycopy(type.bytes, type.offset, ref.bytes, 0, type.length);
            ref.offset = type.length;
            ref.bytes[ref.offset++] = DELIMITER_BYTE;
            System.arraycopy(id.bytes, id.offset, ref.bytes, ref.offset, id.length);
            ref.offset = 0;
            ref.length = ref.bytes.length;
            return ref;
        }
    }

    public static BytesRef[] createUidsForTypesAndId(Collection<String> types, Object id, boolean singleType) {
        return createUidsForTypesAndIds(types, Collections.singletonList(id), singleType);
    }

    public static BytesRef[] createUidsForTypesAndIds(Collection<String> types, Collection<?> ids, boolean singleType) {
        if (singleType && types.size() > 1) {
            throw new IllegalStateException("Multiple types: " + types);
        }
        BytesRef[] uids = new BytesRef[types.size() * ids.size()];
        BytesRefBuilder typeBytes = new BytesRefBuilder();
        BytesRefBuilder idBytes = new BytesRefBuilder();
        int index = 0;
        for (String type : types) {
            typeBytes.copyChars(type);
            for (Object id : ids) {
                uids[index++] = Uid.createUidAsBytes(typeBytes.get(), BytesRefs.toBytesRef(id, idBytes), singleType);
            }
        }
        return uids;
    }

    public static String createUid(String type, String id, boolean singleType) {
        if (singleType) {
            return id;
        } else {
            return type + DELIMITER + id;
        }
    }

}
