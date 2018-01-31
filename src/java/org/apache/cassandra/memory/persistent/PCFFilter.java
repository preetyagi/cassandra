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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;

import lib.util.persistent.PersistentArrayList;
import lib.util.persistent.PersistentSkipListMap;
import lib.util.persistent.PersistentString;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.memory.MCFFilter;
import org.apache.cassandra.memory.MCFSortedMap;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MComplexCell;
import org.apache.cassandra.memory.MConfig;
import org.apache.cassandra.memory.MDataTypes;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MSimpleCell;
import org.apache.cassandra.memory.MTableMetadata;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class PCFFilter implements MCFFilter
{
    TableMetadata metadata;
    private Row.Builder builder;

    public PCFFilter(TableMetadata metadata)
    {
        this.metadata = metadata;
    }

    /*
    Read partitions with multiple row
     */
    @Override
    public Unfiltered getUnfilteredRowIterator(Map.Entry<MClusteringKey, MRow> persistentRowEntry,
                                               TableMetadata metadata)
    {
        builder = BTreeRow.sortedBuilder();
        ByteBuffer[] clusteringValues = new ByteBuffer[persistentRowEntry.getKey().getNumKeys()];
        //create new clustering info
        for (int i = 0; i < persistentRowEntry.getKey().getNumKeys(); i++)
        {
            clusteringValues[i] = ByteBuffer.wrap(persistentRowEntry.getKey().getValueBuf(i));
        }

        builder.newRow(Clustering.make(clusteringValues));
        MRow persistentRow = persistentRowEntry.getValue();
        /* populate cells and add to row */
        ConcurrentNavigableMap<PersistentString, PersistentCellBase> cells = persistentRow.getCells();
        for (Map.Entry<PersistentString, PersistentCellBase> e : ((PersistentSkipListMap<PersistentString, PersistentCellBase>) cells).cachedEntrySet())
        {
            ColumnMetadata column = metadata.getColumn(ByteBuffer.wrap(e.getKey().getBytes()));
            assert column != null : "column is null";
            if (column.isSimple())
            {

                MSimpleCell pcell = (MSimpleCell) (((Map.Entry) e).getValue());
                Cell cassCell;
                if (pcell.getValue() != null)
                    cassCell = new BufferCell(column, pcell.getTimestamp(), pcell.getTtl(), pcell.getLocalDeletionTime(),
                                              ByteBuffer.wrap(pcell.getValue()), null);
                else
                    cassCell = new BufferCell(column, pcell.getTimestamp(), pcell.getTtl(), pcell.getLocalDeletionTime(),
                                              ByteBufferUtil.EMPTY_BYTE_BUFFER, null);
                builder.addCell(cassCell);
            }
            else if (column.isComplex())
            {

                MComplexCell persistentComplexCell = (MComplexCell) (((Map.Entry) e).getValue());
                for (MSimpleCell pcell : (PersistentArrayList<PersistentCell>) persistentComplexCell.getCells())
                {

                    Cell cassCell;
                    if (pcell.getValue() != null)
                        cassCell = new BufferCell(column, pcell.getTimestamp(), pcell.getTtl(), pcell.getLocalDeletionTime(),
                                                  ByteBuffer.wrap(pcell.getValue()),
                                                  CellPath.create(ByteBuffer.wrap(pcell.getCellPath())));
                    else
                        cassCell = new BufferCell(column, pcell.getTimestamp(), pcell.getTtl(), pcell.getLocalDeletionTime(),
                                                  ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                  CellPath.create(ByteBuffer.wrap(pcell.getCellPath())));

                    builder.addCell(cassCell);
                }
            }
        }
        return builder.build();
    }

    /*
    Read partitions with single row
     */
    //TODO: Need to consolidate common functionality
    @Override
    public Unfiltered getUnfilteredRowSingle(MRow mRow, Clustering clustering, TableMetadata metadata)
    {
        builder = BTreeRow.sortedBuilder();
        if (clustering != null)
            builder.newRow(clustering);
        else
            builder.newRow(Clustering.EMPTY);

        ConcurrentNavigableMap<PersistentString, PersistentCellBase> cells = mRow.getCells();
        for (Map.Entry<PersistentString, PersistentCellBase> columnData : cells.entrySet())
        {
            ColumnMetadata column = metadata.getColumn(ByteBuffer.wrap(columnData.getKey().getBytes()));
            assert column != null : "column is null";
            if (column.isSimple())
            {

                MSimpleCell pcell = (MSimpleCell) columnData.getValue();
                Cell cassCell;
                if (pcell.getValue() != null)
                    cassCell = new BufferCell(column, pcell.getTimestamp(), pcell.getTtl(), pcell.getLocalDeletionTime(),
                                              ByteBuffer.wrap(pcell.getValue()), null);
                else
                    cassCell = new BufferCell(column, pcell.getTimestamp(), pcell.getTtl(), pcell.getLocalDeletionTime(),
                                              ByteBufferUtil.EMPTY_BYTE_BUFFER, null);

                builder.addCell(cassCell);
            }
            else if (column.isComplex())
            {

                PersistentComplexCell persistentComplexCell = (PersistentComplexCell) columnData.getValue();
                for (MSimpleCell pcell : persistentComplexCell.getCells())
                {
                    Cell cassCell;
                    if (pcell.getValue() != null)
                        cassCell = new BufferCell(column, pcell.getTimestamp(), pcell.getTtl(), pcell.getLocalDeletionTime(),
                                                  ByteBuffer.wrap(pcell.getValue()),
                                                  CellPath.create(ByteBuffer.wrap(pcell.getCellPath())));
                    else
                        cassCell = new BufferCell(column, pcell.getTimestamp(), pcell.getTtl(), pcell.getLocalDeletionTime(),
                                                  ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                  CellPath.create(ByteBuffer.wrap(pcell.getCellPath())));
                    builder.addCell(cassCell);
                }
            }
        }
        return builder.build();
    }

    /*
    Filter based on ClusteringIndexNames
     */
    //TODO: Works for now. Need to improve & remove hard-coded stuff
    @Override
    public UnfilteredRowIterator getRowIterator(ClusteringIndexNamesFilter filter, Object persistentVal, DecoratedKey dKey)
    {

        NavigableSet<Clustering> clusteringSet = filter.requestedRows();
        Iterator clusteringIterator = clusteringSet.iterator();
        return new AbstractUnfilteredRowIterator(metadata,
                                                 dKey,
                                                 DeletionTime.LIVE,
                                                 metadata.regularAndStaticColumns(),
                                                 Rows.EMPTY_STATIC_ROW,
                                                 false,
                                                 EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                while (clusteringIterator.hasNext())
                {
                    Clustering c = (Clustering) clusteringIterator.next();
                    ByteBuffer[] bbArray = c.getRawValues();
                    List<ByteBuffer> bbList = Arrays.asList(bbArray);

                    MClusteringKey pclKey = MClusteringKey.getInstance();
                    assert pclKey != null : "pclKey is null";
                    pclKey.addClusteringKeyColumn("table_name", MDataTypes.getType(MConfig.PM_UTF8_TYPE),
                                                  bbList.get(0).array());

                    MRow pRow = ((PersistentColumnFamilySortedMap) persistentVal).get(pclKey);
                    if (pRow != null)
                    {
                        return getUnfilteredRowSingle(pRow, c, metadata);
                    }
                }
                return endOfData();
            }
        };
    }

    /*
    Filter based on Slice information
     */
    //TODO: Works for now. Need to improve & remove hard-coded stuff
    @Override
    public UnfilteredRowIterator getSliceIterator(ClusteringIndexFilter filter, Object persistentVal, TableMetadata metadata,
                                                  MTableMetadata mTableMetadata, DecoratedKey decoratedKey)
    {
        Iterator<Map.Entry<PersistentClusteringKey, PersistentRow>> pcfMapIterator =
        ((PersistentColumnFamilySortedMap) persistentVal).getSortedMap().entrySet().iterator();

        return new AbstractUnfilteredRowIterator(metadata,
                                                 decoratedKey,
                                                 DeletionTime.LIVE,
                                                 metadata.regularAndStaticColumns(),
                                                 Rows.EMPTY_STATIC_ROW,
                                                 false,
                                                 EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                while (pcfMapIterator.hasNext())
                {

                    Map.Entry<PersistentClusteringKey, PersistentRow> persistentClusteringKeyMap = pcfMapIterator.next();
                    MClusteringKey persistentClusteringKey = persistentClusteringKeyMap.getKey();
                    MRow pRow = persistentClusteringKeyMap.getValue();
                    if (filter.getSlices(metadata).hasUpperBound())
                    {
                        ByteBuffer startSlice;
                        startSlice = filter.getSlices(metadata).get(0).start().get(0);
                        assert startSlice != null : "Slice is null.";

                        MClusteringKey startPersistentClusteringKey = MClusteringKey.getInstance();
                        assert startPersistentClusteringKey != null : "PersistentClusteringKey is null.";

                        startPersistentClusteringKey.addClusteringKeyColumn("table_name",
                                                                            MDataTypes.getType(MConfig.PM_UTF8_TYPE),
                                                                            startSlice.array());

                        if (Arrays.equals(persistentClusteringKey.getValueBuf(0),
                                          startPersistentClusteringKey.getValueBuf(0)))
                        {
                            ByteBuffer[] clusteringValues = new ByteBuffer[persistentClusteringKey.getNumKeys()];
                            //create new clustering info
                            for (int i = 0; i < persistentClusteringKey.getNumKeys(); i++)
                            {
                                clusteringValues[i] = ByteBuffer.wrap(persistentClusteringKey.getValueBuf(i));
                            }
                            return getUnfilteredRowSingle(pRow, Clustering.make(clusteringValues), metadata);
                        }
                    }
                    else
                        return getUnfilteredRowSingle(pRow,
                                                      Clustering.make(ByteBuffer.wrap(persistentClusteringKey.getValueBuf(0))),
                                                      metadata);
                }
                return endOfData();
            }
        };
    }
}
