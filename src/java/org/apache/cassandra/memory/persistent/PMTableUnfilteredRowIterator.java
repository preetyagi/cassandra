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
import java.util.UUID;

import lib.util.persistent.PersistentArrayList;
import lib.util.persistent.PersistentString;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.memory.MCFSortedMap;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MColumnMetadata;
import org.apache.cassandra.memory.MDataTypes;
import org.apache.cassandra.memory.MListType;
import org.apache.cassandra.memory.MMapType;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MRowSingle;
import org.apache.cassandra.memory.MSetType;
import org.apache.cassandra.memory.MTableMetadata;
import org.apache.cassandra.memory.MTableUnfilteredRowIterator;
import org.apache.cassandra.memory.MUtils;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

/* An iterator for PersistentColumnFamilySortedMap */

public class PMTableUnfilteredRowIterator extends AbstractIterator<Unfiltered> implements MTableUnfilteredRowIterator
{

    PCFFilter filter;
    private DecoratedKey dKey;
    private TableMetadata metadata;
    private MTableMetadata persistentTableMetadata;
    private MCFSortedMap pcfMap;
    private MRowSingle pRow;
    private Iterator<PersistentRow> pcfRowIterator;
    private Iterator<Map.Entry<MClusteringKey, MRow>> pcfIterator;
    private boolean isClusteringAvailable;

    public PMTableUnfilteredRowIterator(ByteBuffer key, TableMetadata metadata, MTableMetadata persistentTableMetadata, MCFSortedMap pcfSortedMap)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        this.dKey = partitioner.decorateKey(key);
        this.persistentTableMetadata = persistentTableMetadata;
        if (metadata == null)
            this.metadata = getTableMetadataFromPM(persistentTableMetadata);
        else
            this.metadata = metadata;
        this.filter = new PCFFilter(metadata);
        this.pcfMap = pcfSortedMap;
        this.pcfIterator = pcfMap.getSortedMap().entrySet().iterator();
        isClusteringAvailable = true;

    }

    public PMTableUnfilteredRowIterator(ByteBuffer key, TableMetadata metadata, MTableMetadata persistentTableMetadata, MRowSingle pRowSingle)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        this.dKey = partitioner.decorateKey(key);
        this.persistentTableMetadata = persistentTableMetadata;
        if (metadata == null)
            this.metadata = getTableMetadataFromPM(persistentTableMetadata);
        else
            this.metadata = metadata;
        this.filter = new PCFFilter(metadata);
        this.pRow = pRowSingle;
        this.pcfRowIterator = pRow.getRowIterator();
        isClusteringAvailable = false;

    }

    private TableMetadata getTableMetadataFromPM(MTableMetadata persistentTableMetadata)
    {
        TableMetadata.Builder builder =
        TableMetadata.builder(persistentTableMetadata.getKeyspace(), persistentTableMetadata.getTableName(),
                              TableId.fromUUID(new UUID(persistentTableMetadata.getTableId().getMostSignificantBits(),
                                                        persistentTableMetadata.getTableId().getLeastSignificantBits())));
        for (MColumnMetadata columnMetadata : (PersistentArrayList<PersistentColumnMetadata>) persistentTableMetadata.getPartitionKey())
        {
            MDataTypes dataType = columnMetadata.getDataType();
            if (dataType instanceof MMapType)
            {
                AbstractType keys, values;
                boolean isMultiCell = columnMetadata.getIsMultiCell();
                keys = MUtils.getTypeFromMData(((MMapType) dataType).getKeyType());
                values = MUtils.getTypeFromMData(((MMapType) dataType).getValType());
                builder.addPartitionKeyColumn(columnMetadata.getColumnName(), MapType.getInstance(keys, values, isMultiCell));
            }
            else if (dataType instanceof MSetType)
            {
                AbstractType elements;
                boolean isMultiCell = columnMetadata.getIsMultiCell();
                elements = MUtils.getTypeFromMData(((MSetType) dataType).getElementType());
                builder.addPartitionKeyColumn(columnMetadata.getColumnName(), SetType.getInstance(elements, isMultiCell));
            }
            else if (dataType instanceof MListType)
            {
                AbstractType values;
                boolean isMultiCell = columnMetadata.getIsMultiCell();
                values = MUtils.getTypeFromMData(((MListType) dataType).getElementType());
                builder.addPartitionKeyColumn(columnMetadata.getColumnName(), ListType.getInstance(values, isMultiCell));
            }
            else
                builder.addPartitionKeyColumn(columnMetadata.getColumnName(), MUtils.getTypeFromMData(columnMetadata.getDataType()));
        }
        for (MColumnMetadata columnMetadata : (PersistentArrayList<PersistentColumnMetadata>) persistentTableMetadata.getClusteringKey())
        {
            MDataTypes dataType = columnMetadata.getDataType();
            if (dataType instanceof MMapType)
            {
                AbstractType keys, values;
                boolean isMultiCell = columnMetadata.getIsMultiCell();
                keys = MUtils.getTypeFromMData(((MMapType) dataType).getKeyType());
                values = MUtils.getTypeFromMData(((MMapType) dataType).getValType());

                builder.addClusteringColumn(columnMetadata.getColumnName(), MapType.getInstance(keys, values, isMultiCell));
            }
            else if (dataType instanceof MSetType)
            {
                AbstractType elements;
                boolean isMultiCell = columnMetadata.getIsMultiCell();
                elements = MUtils.getTypeFromMData(((MSetType) dataType).getElementType());
                builder.addClusteringColumn(columnMetadata.getColumnName(), SetType.getInstance(elements, isMultiCell));
            }
            else if (dataType instanceof MListType)
            {
                AbstractType values;
                boolean isMultiCell = columnMetadata.getIsMultiCell();
                values = MUtils.getTypeFromMData(((MListType) dataType).getElementType());
                builder.addClusteringColumn(columnMetadata.getColumnName(), ListType.getInstance(values, isMultiCell));
            }
            else
                builder.addClusteringColumn(columnMetadata.getColumnName(), MUtils.getTypeFromMData(columnMetadata.getDataType()));
        }

        //TODO: Need to address static column
        for (Map.Entry<PersistentString, PersistentColumnMetadata> columnMetadata :
        ((PersistentTableMetadata) persistentTableMetadata).getRegularStaticColumns().entrySet())
        {
            MDataTypes dataType = columnMetadata.getValue().getDataType();
            if (dataType instanceof MMapType)
            {
                AbstractType keys, values;
                boolean isMultiCell = columnMetadata.getValue().getIsMultiCell();
                keys = MUtils.getTypeFromMData(((MMapType) dataType).getKeyType());
                values = MUtils.getTypeFromMData(((MMapType) dataType).getValType());
                builder.addRegularColumn(columnMetadata.getValue().getColumnName(), MapType.getInstance(keys, values, isMultiCell));
            }
            else if (dataType instanceof MSetType)
            {
                AbstractType elements;
                boolean isMultiCell = columnMetadata.getValue().getIsMultiCell();
                elements = MUtils.getTypeFromMData(((MSetType) dataType).getElementType());
                builder.addRegularColumn(columnMetadata.getValue().getColumnName(), SetType.getInstance(elements, isMultiCell));
            }
            else if (dataType instanceof MListType)
            {
                AbstractType elements;
                boolean isMultiCell = columnMetadata.getValue().getIsMultiCell();
                elements = MUtils.getTypeFromMData(((MListType) dataType).getElementType());
                builder.addRegularColumn(columnMetadata.getValue().getColumnName(), ListType.getInstance(elements, isMultiCell));
            }
            else
                builder.addRegularColumn(columnMetadata.getValue().getColumnName(),
                                         MUtils.getTypeFromMData(columnMetadata.getValue().getDataType()));
        }

        return builder.build();
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
