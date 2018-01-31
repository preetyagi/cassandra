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

import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.memory.inmemory.VMTableUnfilteredRowIterator;
import org.apache.cassandra.memory.persistent.PMTableUnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

public interface MTableUnfilteredRowIterator extends UnfilteredRowIterator
{

    public static MTableUnfilteredRowIterator getInstance(ByteBuffer keyData, TableMetadata metadata, MTableMetadata persistentTableMetadata, MCFSortedMap pcfSortedMap)
    {
        if (DatabaseDescriptor.isPersistentMemoryEnabled())
            return new PMTableUnfilteredRowIterator(keyData, metadata, persistentTableMetadata, pcfSortedMap);
        else if (DatabaseDescriptor.isInMemoryEnabled())
            return VMTableUnfilteredRowIterator.getInstance(keyData, metadata, pcfSortedMap);
        else
            return null;
    }

    public static MTableUnfilteredRowIterator getInstance(ByteBuffer keyData, TableMetadata metadata, MTableMetadata persistentTableMetadata, MRowSingle rowSingle)
    {
        if (DatabaseDescriptor.isPersistentMemoryEnabled())
            return new PMTableUnfilteredRowIterator(keyData, metadata, persistentTableMetadata, rowSingle);
        else if (DatabaseDescriptor.isInMemoryEnabled())
            return VMTableUnfilteredRowIterator.getInstance(keyData, metadata, rowSingle);
        else
            return null;
    }

    @Override
    public default void close()
    {

    }

    public TableMetadata metadata();

    public boolean isReverseOrder();

    public RegularAndStaticColumns columns();

    public DecoratedKey partitionKey();

    public Row staticRow();

    public DeletionTime partitionLevelDeletion();

    public EncodingStats stats();

    public Unfiltered computeNext();
}
