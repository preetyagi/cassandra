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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentFPTree2;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.types.FinalBooleanField;
import lib.util.persistent.types.FinalObjectField;
import lib.util.persistent.types.ObjectType;
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
import org.apache.cassandra.schema.TableMetadata;

/**
 * This class represents the PMTable for a given Column Family.
 * It maintains the mappings for <Partition Key, Rows> for a given column family.
 */
public final class PMTable extends PersistentImmutableObject implements MTable
{
    private static final FinalObjectField<PersistentFPTree2<PersistentDecoratedKey, PersistentImmutableObject>> PMTABLE = new FinalObjectField<>();
    private static final FinalBooleanField IS_CK_AVAILABLE = new FinalBooleanField();
    private static final FinalObjectField<PersistentTableMetadata> PM_METADATA = new FinalObjectField<>();

    private static final ObjectType<PMTable> TYPE = ObjectType.fromFields(PMTable.class,
                                                                          PMTABLE,
                                                                          IS_CK_AVAILABLE,
                                                                          PM_METADATA);

    public TableMetadata tableMetadata;


    // constructor
    public PMTable(MTableMetadata mTableMetadata, boolean isClusteringKeyAvailable, TableMetadata tableMetadata)
    {
        super(TYPE, (PersistentImmutableObject self) ->
        {
            self.initBooleanField(IS_CK_AVAILABLE, isClusteringKeyAvailable);
            self.initObjectField(PM_METADATA, (PersistentTableMetadata) mTableMetadata);
            self.initObjectField(PMTABLE, new PersistentFPTree2<>());
        });
        this.tableMetadata = tableMetadata;
    }

    // reconstructor
    private PMTable(ObjectPointer<? extends PMTable> pointer)
    {
        super(pointer);
    }

    public PersistentFPTree2<PersistentDecoratedKey, PersistentImmutableObject> getPMTable()
    {
        return getObjectField(PMTABLE);
    }

    @Override
    public MTableMetadata getTableMetadata()
    {
        return getObjectField(PM_METADATA);
    }

    @Override
    public boolean doesClusteringKeyExist()
    {
        return getBooleanField(IS_CK_AVAILABLE);
    }

    // Insert partition key if doesn't exist already.
    @Override
    public void putPartitionKeyIfAbsent(MDecoratedKey partitionPosition)
    {
        if (getBooleanField(IS_CK_AVAILABLE))
        {
            getPMTable().putIfAbsent((PersistentDecoratedKey) partitionPosition,
                                     (PersistentColumnFamilySortedMap) MCFSortedMap.getInstance());
        }
        else
        {
            getPMTable().putIfAbsent((PersistentDecoratedKey) partitionPosition,
                                     (PersistentRowSingle) MRowSingle.getInstance());
        }
    }

    @Override
    public void putRow(MDecoratedKey key, MRow row)
    {
        MRowSingle rowSingle = (MRowSingle) getPMTable().get(key);
        if (rowSingle != null)
            rowSingle.add(row);
    }

    // The return type can either be PersistentRow or PersistentColumnFamilySortedMap
    // TODO: Should capture the case failure in order to avoid server crash
    @Override
    public <T> T get(MDecoratedKey partitionPosition)
    {
        try
        {
            return (T) getPMTable().get(partitionPosition);
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
        try
        {
            return (T) getPMTable().get(partitionPosition, PersistentDecoratedKey.class);
        }
        catch (ClassCastException e)
        {
            e.printStackTrace(); // TODO: log this exception
            return null;
        }
    }

    /* An iterator for PMTable */

    @Override
    public PMTableUnfilteredPartitionIterator makePartitionIterator()
    {
        Iterator<Map.Entry<PersistentDecoratedKey, PersistentImmutableObject>> iter = getPMTable().entrySet().iterator();
        return new PMTableUnfilteredPartitionIterator(iter, tableMetadata, getTableMetadata(), doesClusteringKeyExist());
    }

    public static class PMTableUnfilteredPartitionIterator implements MTableUnfilteredPartitionIterator
    {
        private final Iterator<Map.Entry<PersistentDecoratedKey, PersistentImmutableObject>> iter;
        private TableMetadata tableMetadata;
        private MTableMetadata mTableMetadata;
        private boolean isClusteringKeyAvailable;

        private PMTableUnfilteredPartitionIterator(Iterator<Map.Entry<PersistentDecoratedKey, PersistentImmutableObject>> iter,
                                                   TableMetadata tableMetadata,
                                                   MTableMetadata mTableMetadata,
                                                   boolean isClusteringKeyAvailable)
        {
            this.iter = iter;
            this.tableMetadata = tableMetadata;
            this.mTableMetadata = mTableMetadata;
            this.isClusteringKeyAvailable = isClusteringKeyAvailable;
        }

        public static <T> PMTableUnfilteredPartitionIterator getInstance(T iter,
                                                                         TableMetadata tableMetadata,
                                                                         MTableMetadata mTableMetadata,
                                                                         boolean isClusteringKeyAvailable)
        {
            return new PMTableUnfilteredPartitionIterator((Iterator<Map.Entry<PersistentDecoratedKey, PersistentImmutableObject>>) iter, tableMetadata, mTableMetadata, isClusteringKeyAvailable);
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
            Map.Entry<PersistentDecoratedKey, PersistentImmutableObject> entry = iter.next();
            assert entry != null;
            PersistentDecoratedKey pKey = entry.getKey();

            if (isClusteringKeyAvailable)
            {
                return new PMTableUnfilteredRowIterator(ByteBuffer.wrap(pKey.getKey()),
                                                        tableMetadata,
                                                        mTableMetadata,
                                                        ((MCFSortedMap) entry.getValue()));
            }
            else
            {
                return new PMTableUnfilteredRowIterator(ByteBuffer.wrap(pKey.getKey()),
                                                        tableMetadata,
                                                        mTableMetadata,
                                                        ((MRowSingle) entry.getValue()));
            }
        }
    }
}
