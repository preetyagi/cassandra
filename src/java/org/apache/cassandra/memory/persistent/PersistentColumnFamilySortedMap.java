/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.memory.persistent;

import java.util.concurrent.ConcurrentNavigableMap;

import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.PersistentSkipListMap;
import lib.util.persistent.types.FinalObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.memory.MCFSortedMap;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MRow;

/**
 * Represents the sorted group of column families (rows)
 * in a given partition. The sorting is done based on the
 * clustering key.
 */
public final class PersistentColumnFamilySortedMap extends PersistentImmutableObject implements MCFSortedMap
{
    private static final FinalObjectField<PersistentSkipListMap<PersistentClusteringKey, PersistentRow>> CF_SORTED_MAP = new FinalObjectField<>();

    private static final ObjectType<PersistentColumnFamilySortedMap> TYPE = ObjectType.fromFields
                                                                                       (PersistentColumnFamilySortedMap.class, CF_SORTED_MAP);

    // constructor
    public PersistentColumnFamilySortedMap()
    {
        super(TYPE, (PersistentImmutableObject self) ->
        {
            self.initObjectField(CF_SORTED_MAP, new PersistentSkipListMap<>());
        });
    }

    // reconstructor
    private PersistentColumnFamilySortedMap(ObjectPointer<? extends PersistentColumnFamilySortedMap> pointer)
    {
        super(pointer);
    }

    @Override
    public ConcurrentNavigableMap<PersistentClusteringKey, PersistentRow> getSortedMap()
    {
        return getObjectField(CF_SORTED_MAP);
    }

    @Override
    public void put(MClusteringKey clusteringKey, MRow row)
    {
        PersistentRow pRow = (PersistentRow) get(clusteringKey);
        if (pRow == null)
            getSortedMap().put((PersistentClusteringKey) clusteringKey, (PersistentRow) row); // update CF in-place
        else
        {
            pRow.getCells().putAll(row.getCells());
            pRow.setType(row.getType());
            pRow.setFlags(row.getFlags());
            pRow.setExtendedFlags(row.getExtendedFlags());
            pRow.setPrimaryKeyLivenessTimestamp(row.getPrimaryKeyLivenessTimestamp());
            pRow.setPkLocalExpirationTime(row.getPkLocalExpirationTime());
            if (row.getDeletionTime() != null)
                pRow.setDeletionTime(row.getDeletionTime());
        }
    }

    @Override
    public MRow get(MClusteringKey clusteringKey)
    {
        return getSortedMap().get(clusteringKey);
    }
}
