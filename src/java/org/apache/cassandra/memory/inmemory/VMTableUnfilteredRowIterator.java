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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.memory.MCFSortedMap;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MRowSingle;
import org.apache.cassandra.memory.MTableMetadata;
import org.apache.cassandra.memory.MTableUnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

/* An iterator for PersistentColumnFamilySortedMap */

public class VMTableUnfilteredRowIterator extends AbstractIterator<Unfiltered> implements MTableUnfilteredRowIterator
{

    VMCFFilter filter;
    private DecoratedKey dKey;
    private TableMetadata metadata;
    private MCFSortedMap pcfMap;
    private Iterator<Map.Entry<MClusteringKey, MRow>> pcfIterator;
    private MRowSingle pRow;
    private Iterator<MRow> pcfRowIterator;
    private boolean isClusteringAvailable;

    private VMTableUnfilteredRowIterator(ByteBuffer key, TableMetadata metadata, MCFSortedMap pcfSortedMap)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        this.dKey = partitioner.decorateKey(key);
        this.metadata = metadata;
        this.pcfMap = pcfSortedMap;
        this.pcfIterator = pcfMap.getSortedMap().entrySet().iterator();
        isClusteringAvailable = true;
        this.filter = new VMCFFilter(metadata);
    }

    private VMTableUnfilteredRowIterator(ByteBuffer key, TableMetadata metadata, MRowSingle pRowSingle)
    {

        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        this.dKey = partitioner.decorateKey(key);
        this.metadata = metadata;
        this.pRow = pRowSingle;
        this.pcfRowIterator = pRow.getRowIterator();
        isClusteringAvailable = false;
        this.filter = new VMCFFilter(metadata);
    }

    public static VMTableUnfilteredRowIterator getInstance(ByteBuffer key, TableMetadata metadata, MCFSortedMap pcfSortedMap)
    {
        return new VMTableUnfilteredRowIterator(key, metadata, pcfSortedMap);
    }

    public static VMTableUnfilteredRowIterator getInstance(ByteBuffer key, TableMetadata metadata, MRowSingle pRowSingle)
    {
        return new VMTableUnfilteredRowIterator(key, metadata, pRowSingle);
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public boolean isReverseOrder()
    {
        return false;
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        if (metadata != null)
            return metadata.regularAndStaticColumns();
        else
            return null;
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return dKey;
    }

    @Override
    public Row staticRow()
    {
        return Rows.EMPTY_STATIC_ROW;
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        return DeletionTime.LIVE;
    }

    @Override
    public EncodingStats stats()
    {
        return EncodingStats.NO_STATS;
    }

    @Override
    public Unfiltered computeNext()
    {
        if (isClusteringAvailable)
        {
            if (this.pcfIterator.hasNext())
            {
                return filter.getUnfilteredRowIterator(pcfIterator.next(), metadata);
            }
        }
        else
        {
            if (pcfRowIterator.hasNext())
            {
                return filter.getUnfilteredRowSingle(pcfRowIterator.next(), null, metadata);
            }
        }
        return endOfData();
    }
}
