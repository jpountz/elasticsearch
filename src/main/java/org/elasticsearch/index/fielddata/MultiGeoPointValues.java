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
package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.geo.GeoPoint;

/**
 * A state-full lightweight per document set of {@link GeoPoint} values.
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   GeoPointValues values = ..;
 *   final int numValues = values.setDocId(docId);
 *   for (int i = 0; i < numValues; i++) {
 *       GeoPoint value = values.nextValue();
 *       // process value
 *   }
 * </pre>
 */
public abstract class MultiGeoPointValues {

    /**
     * Creates a new {@link MultiGeoPointValues} instance
     */
    protected MultiGeoPointValues() {
    }

    /**
     * Sets iteration to the specified docID and returns the number of
     * values for this document ID,
     * @param docId document ID
     *
     * @see #nextValue()
     */
    public abstract void setDocument(int docId);

    /**
     * Return the number of geo points this document has.
     */
    public abstract int count();

    /**
     * Returns the next value for the current docID set to {@link #setDocument(int)}.
     * This method should only be called <tt>N</tt> times where <tt>N</tt> is the number
     * returned from {@link #setDocument(int)}. If called more than <tt>N</tt> times the behavior
     * is undefined.
     * <p>
     * If this instance returns ordered values the <tt>Nth</tt> value is strictly less than the <tt>N+1</tt> value with
     * respect to the {@link AtomicFieldData.Order} returned from {@link #getOrder()}. If this instance returns
     * <i>unordered</i> values {@link #getOrder()} must return {@link AtomicFieldData.Order#NONE}
     * Note: the values returned are de-duplicated, only unique values are returned.
     * </p>
     *
     * Note: the returned {@link GeoPoint} might be shared across invocations.
     *
     * @return the next value for the current docID set to {@link #setDocument(int)}.
     */
    public abstract GeoPoint valueAt(int index);

}
