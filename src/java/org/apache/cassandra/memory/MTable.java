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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.memory.inmemory.VMTable;
import org.apache.cassandra.memory.persistent.PMTable;
import org.apache.cassandra.schema.TableMetadata;

public interface MTable
{
    /**
     * Create {@link PMTable} or {@link VMTable} instance
     *
     * @param mTableMetadata
     * @param isClusteringKeyAvailable
     * @param tableMetadata
     * @return persistent or volatile table instance
     */
    public static MTable getInstance(MTableMetadata mTableMetadata,
                                     boolean isClusteringKeyAvailable,
                                     TableMetadata tableMetadata)
    {
        if (DatabaseDescriptor.isPersistentMemoryEnabled())
            return new PMTable(mTableMetadata, isClusteringKeyAvailable, tableMetadata);
        else if (DatabaseDescriptor.isInMemoryEnabled())
            return VMTable.getInstance(isClusteringKeyAvailable, tableMetadata);

        return null;
    }

    /**
     * Get table metadata from in-memory storage
     *
     * @return {@link org.apache.cassandra.memory.persistent.PersistentTableMetadata} or
     * {@link org.apache.cassandra.memory.inmemory.VMTableMetadata}
     */
    public MTableMetadata getTableMetadata();

    /**
     * Check if the table has clustering key(s)
     *
     * @return
     */
    public boolean doesClusteringKeyExist();

    /**
     * Insert partition key if doesn't exist already
     *
     * @param partitionPosition
     */
    public void putPartitionKeyIfAbsent(MDecoratedKey partitionPosition);

    /**
     * Insert a row directly if there is no clustering key
     *
     * @param key
     * @param row
     */
    public void putRow(MDecoratedKey key, MRow row);

    /**
     * Get row(s) for the given partition
     *
     * @param partitionPosition
     * @return {@link MCFSortedMap} or {@link MRowSingle} depending on the schema
     */
    public <T> T get(MDecoratedKey partitionPosition);

    /**
     * Overloaded function which consumes Cassandra's {@link DecoratedKey} instance to
     * look up and return row(s) for a given partition key
     *
     * @param partitionPosition
     * @return {@link MCFSortedMap} or {@link MRowSingle} depending on the schema
     */
    public <T> T get(DecoratedKey partitionPosition);

    /**
     * Prepares an iterator for all partitions
     *
     * @return partition iterator
     */
    public MTableUnfilteredPartitionIterator makePartitionIterator();
}
