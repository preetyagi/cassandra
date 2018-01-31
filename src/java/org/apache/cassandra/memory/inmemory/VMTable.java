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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.memory.MCFSortedMap;
import org.apache.cassandra.memory.MDecoratedKey;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MRowSingle;
import org.apache.cassandra.memory.MTable;
import org.apache.cassandra.memory.MTableMetadata;
import org.apache.cassandra.memory.MTableUnfilteredPartitionIterator;
import org.apache.cassandra.memory.MTableUnfilteredRowIterator;
import org.apache.cassandra.memory.MToken;
import org.apache.cassandra.schema.TableMetadata;

/**
 * This class represents the PMTable for a given Column Family.
 * It maintains the mappings for <Partition Key, Rows> for a given column family.
 */
public final class VMTable implements MTable
{
    public ConcurrentSkipListMap<MDecoratedKey, Object> imTable;

    public TableMetadata tableMetadata;
    public boolean isClusteringKeyAvailable;

    // constructor
    private VMTable(boolean isClusteringKeyAvailable, TableMetadata tableMetadata)
    {
        this.isClusteringKeyAvailable = isClusteringKeyAvailable;
        this.tableMetadata = tableMetadata;
        this.imTable = new ConcurrentSkipListMap<>();
    }

    public static VMTable getInstance(boolean isClusteringKeyAvailable,
                                      TableMetadata tableMetadata)
    {
        return new VMTable(isClusteringKeyAvailable, tableMetadata);
    }

    @Override
    public MTableMetadata getTableMetadata()
    {
        return null;
    }

    @Override
    public boolean doesClusteringKeyExist()
    {
        return isClusteringKeyAvailable;
    }

    @Override
    public void putPartitionKeyIfAbsent(MDecoratedKey partitionPosition)
    {
        if (isClusteringKeyAvailable)
        {
            imTable.putIfAbsent(partitionPosition, MCFSortedMap.getInstance());
        }
        else
        {
            imTable.putIfAbsent(partitionPosition, MRowSingle.getInstance());
        }
    }

    @Override
    public void putRow(MDecoratedKey key, MRow row)
    {
        MRowSingle rowSingle = (MRowSingle) imTable.get(key);
        if (rowSingle != null)
            rowSingle.add(row);
    }

    /*public void putRow(DecoratedKey key, MRow row)
    {
        ((MRowSingle) imTable.get(key)).add(row);
    }*/

    // The return type can either be VMRow or VMColumnFamilySortedMap
    // TODO: Should capture the case failure in order to avoid server crash
    @Override
    public <T> T get(MDecoratedKey partitionPosition)
    {
        try
        {
            return (T) imTable.get(partitionPosition);
        }
        catch (ClassCastException e)
        {
            e.printStackTrace(); // TODO: log this exception
            return null;
        }
    }

    @Override
    public <T> T get(DecoratedKey partitionPosition)
    {
        // TODO: Temporary code to avoid crash. Make it work without creating MDecoratedKey instance
        MToken token = MToken.getInstance(partitionPosition.getToken().getTokenValue());
        MDecoratedKey decoratedKey = MDecoratedKey.getInstance(partitionPosition.getKey().array(), partitionPosition, token);
        try
        {
            return (T) imTable.get(decoratedKey);
        }
        catch (ClassCastException e)
        {
            e.printStackTrace(); // TODO: log this exception
            return null;
        }
    }

    /* An iterator for PMTable */

    public MTableUnfilteredPartitionIterator makePartitionIterator()
    {
        Iterator<Map.Entry<MDecoratedKey, Object>> iter = imTable.entrySet().iterator();
        return new IMTableUnfilteredPartitionIterator(iter, tableMetadata, getTableMetadata(), doesClusteringKeyExist());
    }

    public static class IMTableUnfilteredPartitionIterator implements MTableUnfilteredPartitionIterator
    {
        private final Iterator<Map.Entry<MDecoratedKey, Object>> iter;
        private TableMetadata tableMetadata;
        private MTableMetadata mTableMetadata;
        private boolean isClusteringKeyAvailable;

        public IMTableUnfilteredPartitionIterator(Iterator<Map.Entry<MDecoratedKey, Object>> iter,
                                                  TableMetadata tableMetadata,
                                                  MTableMetadata mTableMetadata,
                                                  boolean isClusteringKeyAvailable)
        {
            this.iter = iter;
            this.tableMetadata = tableMetadata;
            this.mTableMetadata = mTableMetadata;
            this.isClusteringKeyAvailable = isClusteringKeyAvailable;
        }

        public static <T> IMTableUnfilteredPartitionIterator getInstance(T iter,
                                                                         TableMetadata tableMetadata,
                                                                         MTableMetadata mTableMetadata,
                                                                         boolean isClusteringKeyAvailable)
        {
            return new IMTableUnfilteredPartitionIterator((Iterator<Map.Entry<MDecoratedKey, Object>>) iter, tableMetadata, mTableMetadata, isClusteringKeyAvailable);
        }

        @Override
        public TableMetadata metadata()
        {
            return tableMetadata;
        }

        @Override
        public boolean hasNext()
        {
            return iter.hasNext();
        }

        @Override
        public UnfilteredRowIterator next()
        {
            Map.Entry<MDecoratedKey, Object> entry = iter.next();
            assert entry.getKey() != null : "decorated key is null.";
            MDecoratedKey pKey = entry.getKey();

            if (isClusteringKeyAvailable)
            {
                return MTableUnfilteredRowIterator.getInstance(ByteBuffer.wrap(pKey.getKey()),
                                                               tableMetadata,
                                                               mTableMetadata,
                                                               ((MCFSortedMap) entry.getValue()));
            }
            else
            {
                return MTableUnfilteredRowIterator.getInstance(ByteBuffer.wrap(pKey.getKey()),
                                                               tableMetadata,
                                                               mTableMetadata,
                                                               ((MRowSingle) (entry.getValue()
                                                               )));
            }
        }
    }
}
