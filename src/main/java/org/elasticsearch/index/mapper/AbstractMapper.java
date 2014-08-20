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


/**
 * Base implementation for a {@link Mapper}.
 */
public abstract class AbstractMapper implements Mapper, Cloneable {

    protected AbstractMapper clone() {
        try {
            return (AbstractMapper) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(); // cannot happen since we implement Cloneable
        }
    }

    protected abstract String contentType();

    @Override
    public final AbstractMapper merge(Mapper mergeWith, MergeContext mergeContext) {
        if (!this.getClass().equals(mergeWith.getClass())) {
            String mergedType = mergeWith.getClass().getSimpleName();
            if (mergeWith instanceof AbstractMapper) {
                mergedType = ((AbstractMapper) mergeWith).contentType();
            }
            mergeContext.addConflict("mapper [" + name() + "] of different type, current_type [" + contentType() + "], merged_type [" + mergedType + "]");
            // different types, return
            return this;
        }

        AbstractMapper clone = clone();
        clone.doMerge(mergeWith, mergeContext);
        return clone;
    }

    protected void doMerge(Mapper mergeWith, MergeContext mergeContext) {
        // nothing by default
    }

}
