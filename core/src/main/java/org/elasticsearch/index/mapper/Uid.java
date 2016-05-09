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
import org.elasticsearch.Version;
import org.elasticsearch.common.Base64;

import java.io.IOException;

/**
 *
 */
public final class Uid {

    public static final char DELIMITER = '#';

    private final String type;

    private final String id;

    public Uid(String type, String id) {
        this.type = type;
        this.id = id;
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
        return createUid(type, id);
    }

    public static Uid createUid(String uid) {
        int delimiterIndex = uid.indexOf(DELIMITER); // type is not allowed to have # in it..., ids can
        return new Uid(uid.substring(0, delimiterIndex), uid.substring(delimiterIndex + 1));
    }

    public static String createUid(String type, String id) {
        return type + DELIMITER + id;
    }

    static boolean isUrlSafeBase64(String s) {
        if (s.length() % 4 != 0) {
            // base64 strings must have a number of chars that is multiple of 4
            return false;
        }
        int numberOfTrailingEqualSigns = 0;
        for (int i = s.length() - 1; i >= 0; --i) {
            if (s.charAt(i) == '=') {
                numberOfTrailingEqualSigns++;
            }
        }
        if (numberOfTrailingEqualSigns > 2) {
            return false;
        }
        for (int i = 0; i < s.length() - numberOfTrailingEqualSigns; ++i) {
            final char c = s.charAt(i);
            if ((c >= 'A' && c <= 'Z')
                    || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9')
                    || c == '-'
                    || c == '_') {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    /** Create a uid term for indexing in the terms dictionary. */
    public BytesRef toIndexTerm(Version indexCreated) {
        BytesRefBuilder builder = new BytesRefBuilder();
        builder.copyChars(type);
        if (indexCreated.before(Version.V_5_0_0)) {
            builder.append((byte) '#');
            builder.append(new BytesRef(id));
        } else {
            if (isUrlSafeBase64(id)) {
                builder.append((byte) '\0');
                byte[] decoded;
                try {
                    decoded = Base64.decode(id, Base64.URL_SAFE);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                builder.append(new BytesRef(decoded));
            } else {
                builder.append((byte) '#');
                builder.append(new BytesRef(id));
            }
        }
        return builder.toBytesRef();
    }

    /** Parse a {@link Uid} that has been retrieved from the terms dictionary. */
    public static Uid parseIndexTerm(Version indexCreated, BytesRef term) {
        int separatorIndex = -1;
        boolean base64 = false;
        for (int i = 0; i < term.length; ++i) {
            byte b = term.bytes[term.offset + i];
            if (b == '\0' && indexCreated.onOrAfter(Version.V_5_0_0)) {
                separatorIndex = i;
                base64 = true;
                break;
            } else if (b == '#') {
                separatorIndex = i;
                break;
            }
        }
        if (separatorIndex < 0) {
            throw new IllegalArgumentException("Missing separator in uid term: " + term);
        }
        String type = new BytesRef(term.bytes, term.offset, separatorIndex).utf8ToString();
        String id;
        if (base64) {
            try {
                id = Base64.encodeBytes(term.bytes, term.offset + separatorIndex + 1, term.length - separatorIndex - 1, Base64.URL_SAFE);
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot oncode to base 64", e);
            }
        } else {
            id = new BytesRef(term.bytes, term.offset + separatorIndex + 1, term.length - separatorIndex - 1).utf8ToString();
        }
        return new Uid(type, id);
    }
}
