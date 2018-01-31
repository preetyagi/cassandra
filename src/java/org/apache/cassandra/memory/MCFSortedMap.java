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

package org.apache.cassandra.memory;

import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.memory.inmemory.VMColumnFamilySortedMap;
import org.apache.cassandra.memory.persistent.PersistentColumnFamilySortedMap;

public interface MCFSortedMap
{
    /**
     * Creates a sorted column family instance for storing clustering key(s) and normal columns
     *
     * @return persistent or volatile column family instance
     */
    public static MCFSortedMap getInstance()
    {
        if (DatabaseDescriptor.isPersistentMemoryEnabled())
            return new PersistentColumnFamilySortedMap();
        else if (DatabaseDescriptor.isInMemoryEnabled())
            return VMColumnFamilySortedMap.getInstance();

        return null;
    }

    /**
     * Get the sorted map which holds all the data in a partition
     *
     * @return return the sorted map with clustering key(s) and normal columns
     */
    public <T extends MClusteringKey, U extends MRow> ConcurrentNavigableMap<T, U> getSortedMap();

    /**
     * Insert a row in a given partition
     *
     * @param clusteringKey clustering key(s)
     * @param row           row with normal columns
     */
    public void put(MClusteringKey clusteringKey, MRow row);

    /**
     * Get row with normal columns for a given clustering key
     *
     * @param clusteringKey
     * @return row
     */
    public MRow get(MClusteringKey clusteringKey);
}
