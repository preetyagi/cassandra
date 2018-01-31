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

package org.apache.cassandra.memory.inmemory;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.memory.MCFSortedMap;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MRow;

/**
 * Represents the sorted group of column families (rows)
 * in a given partition. The sorting is done based on the
 * clustering key.
 */
public final class VMColumnFamilySortedMap implements MCFSortedMap
{
    private ConcurrentSkipListMap<VMClusteringKey, VMRow> cfSortedMap;

    // constructor
    private VMColumnFamilySortedMap()
    {
        cfSortedMap = new ConcurrentSkipListMap<>();
    }

    public static VMColumnFamilySortedMap getInstance()
    {
        return new VMColumnFamilySortedMap();
    }

    @Override
    public ConcurrentNavigableMap<VMClusteringKey, VMRow> getSortedMap()
    {
        return cfSortedMap;
    }

    @Override
    public void put(MClusteringKey clusteringKey, MRow row)
    {
        MRow mRow = get(clusteringKey);
        if (mRow == null)
            getSortedMap().put((VMClusteringKey) clusteringKey, (VMRow) row); // update CF in-place
        else
        {
            mRow.getCells().putAll(row.getCells());
            mRow.setType(row.getType());
            mRow.setFlags(row.getFlags());
            mRow.setExtendedFlags(row.getExtendedFlags());
            mRow.setPrimaryKeyLivenessTimestamp(row.getPrimaryKeyLivenessTimestamp());
            mRow.setPkLocalExpirationTime(row.getPkLocalExpirationTime());
            if (row.getDeletionTime() != null)
                mRow.setDeletionTime(row.getDeletionTime());
        }
    }

    @Override
    public MRow get(MClusteringKey clusteringKey)
    {
        return getSortedMap().get(clusteringKey);
    }
}
