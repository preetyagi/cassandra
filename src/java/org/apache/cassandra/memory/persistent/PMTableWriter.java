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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import lib.util.persistent.PersistentSIHashMap;
import lib.util.persistent.PersistentString;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.memory.MCFSortedMap;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MComplexCell;
import org.apache.cassandra.memory.MDataTypes;
import org.apache.cassandra.memory.MDecoratedKey;
import org.apache.cassandra.memory.MDeletionTime;
import org.apache.cassandra.memory.MHeader;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MSimpleCell;
import org.apache.cassandra.memory.MStorageWrapper;
import org.apache.cassandra.memory.MTable;
import org.apache.cassandra.memory.MTableWriter;
import org.apache.cassandra.memory.MTablesManager;
import org.apache.cassandra.memory.MToken;
import org.apache.cassandra.memory.MUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.SearchIterator;

public class PMTableWriter implements MTableWriter
{
    private MHeader mHeader;

    public PMTableWriter(MHeader mHeader)
    {
        this.mHeader = mHeader;
    }

    private static boolean hasExtendedFlags(Row row)
    {
        return row.isStatic() || row.deletion().isShadowable();
    }

    @Override
    public void write(UnfilteredRowIterator iterator)
    {
        writePartition(iterator);
    }

    private void writePartition(UnfilteredRowIterator iterator)
    {

        byte[] parKey = new byte[iterator.partitionKey().getKey().remaining()];
        iterator.partitionKey().getKey().duplicate().get(parKey);
        MStorageWrapper mStorageWrapper = MStorageWrapper.getInstance();
        assert mStorageWrapper != null : "storage wrapper instance is null";
        MTablesManager pmTablesManager = mStorageWrapper.getMTablesManager(iterator.metadata().keyspace);
        assert pmTablesManager != null : "table manager instance is null";
        MTable pmTable = pmTablesManager.getMTable(iterator.metadata().id.asUUID());
        assert pmTable != null : "pmtable instance is null";

        MToken token = MToken.getInstance(iterator.partitionKey().getToken().getTokenValue());
        MDecoratedKey persistentDecoratedKey = MDecoratedKey.getInstance(parKey, null, token);
        pmTable.putPartitionKeyIfAbsent(persistentDecoratedKey);
        MiscDomainData domainData = new MiscDomainData();
        domainData.mTable = pmTable;
        domainData.partitionKey = persistentDecoratedKey;

        // Get columns metadata so that PersistentString objects used for column names can be used
        // later when creating/storing cells later
        PersistentSIHashMap<PersistentString, PersistentColumnMetadata> pcm = ((PersistentTableMetadata) pmTable.getTableMetadata()).getRegularStaticColumns();
        domainData.pcm = pcm;
        if (pmTable.doesClusteringKeyExist())
        {
            domainData.cfMap = pmTable.get(persistentDecoratedKey);
            for (int i = 0; i < iterator.metadata().clusteringColumns().size(); i++)
            {
                domainData.clusteringColNames.add(iterator.metadata().clusteringColumns().get(i).name.toString());
            }
        }

        // Write rows
        while (iterator.hasNext())
        {
            Unfiltered iter = iterator.next();
            writeRow(iter, domainData);
        }
    }

    private void writeRow(Unfiltered unfiltered, MiscDomainData domainData)
    {
        int flags = 0;
        int extendedFlags = 0;
        if (unfiltered.kind().equals(Unfiltered.Kind.RANGE_TOMBSTONE_MARKER))
            return;
        Row row = (Row) unfiltered;
        boolean isStatic = row.isStatic();
        Columns headerColumns = mHeader.columns(isStatic);
        LivenessInfo pkLiveness = row.primaryKeyLivenessInfo();
        Row.Deletion deletion = row.deletion();
        boolean hasComplexDeletion = row.hasComplexDeletion();
        boolean hasAllColumns = (row.size() == headerColumns.size());
        boolean hasExtendedFlags = hasExtendedFlags(row);

        if (isStatic)
            extendedFlags |= MTableWriter.IS_STATIC;

        if (!pkLiveness.isEmpty())
            flags |= HAS_TIMESTAMP;
        if (pkLiveness.isExpiring())
            flags |= HAS_TTL;
        if (!deletion.isLive())
        {
            flags |= HAS_DELETION;
            if (deletion.isShadowable())
                extendedFlags |= HAS_SHADOWABLE_DELETION;
        }
        if (hasComplexDeletion)
            flags |= HAS_COMPLEX_DELETION;
        if (hasAllColumns)
            flags |= HAS_ALL_COLUMNS;

        if (hasExtendedFlags)
            flags |= EXTENSION_FLAG;

        MClusteringKey clusteringKey = null;
        boolean isClusteringKeyAvailable = false;

        // Prepare clustering key if size is not zero
        if ((domainData.clusteringColNames.size() != 0) && (domainData.clusteringColNames.size() == row.clustering().size()))
        {
            isClusteringKeyAvailable = true;
            if ((!isStatic) && (domainData.clusteringColNames != null) && (domainData.clusteringColNames.size() != 0))
            {
                clusteringKey = MClusteringKey.getInstance();
                assert clusteringKey != null : "clustering key is null";
                for (int i = 0; i < domainData.clusteringColNames.size(); i++)
                {
                    MDataTypes mDataType = MUtils.getMDataType(mHeader.clusteringTypes().get(i));
                    byte[] clusKey = new byte[row.clustering().get(i).remaining()];
                    row.clustering().get(i).duplicate().get(clusKey);
                    clusteringKey.addClusteringKeyColumn(domainData.clusteringColNames.get(i), mDataType, clusKey);
                }
            }
        }

        MRow mRow = MRow.getInstance();
        assert mRow != null : "mRow is null";
        mRow.setFlags((byte) flags);
        mRow.setExtendedFlags((byte) extendedFlags);
        try
        {
            writeCell(row, flags, mRow, domainData);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        if (isClusteringKeyAvailable)
        {
            if (domainData.cfMap == null) return; // sanity check
            domainData.cfMap.put(clusteringKey, mRow);
        }
        else
        {
            if (domainData.mTable == null) return; // sanity check
            domainData.mTable.putRow(domainData.partitionKey, mRow);
        }
    }

    @Inline
    private void writeCell(Row row, int flags, MRow mRow, MiscDomainData domainData)
    throws IOException
    {

        boolean isStatic = row.isStatic();
        Columns headerColumns = mHeader.columns(isStatic);
        LivenessInfo pkLiveness = row.primaryKeyLivenessInfo();
        Row.Deletion deletion = row.deletion();

        if ((flags & HAS_TIMESTAMP) != 0)
        {
            mRow.setPrimaryKeyLivenessTimestamp(pkLiveness.timestamp());
        }

        if ((flags & HAS_TTL) != 0)
        {
            mRow.setPrimaryKeyTTL(pkLiveness.ttl());
            mRow.setPkLocalExpirationTime(pkLiveness.localExpirationTime());
        }

        if ((flags & HAS_DELETION) != 0)
        {
            MDeletionTime deletionTime = MDeletionTime.getInstance(deletion.time().markedForDeleteAt(),
                                                                   deletion.time().localDeletionTime());
            mRow.setDeletionTime(deletionTime);
        }

        SearchIterator<ColumnMetadata, ColumnMetadata> si = headerColumns.iterator();

        Iterator<ColumnData> cellIterator = row.iterator();
        while (cellIterator.hasNext())
        {
            ColumnData cd = cellIterator.next();
            ColumnMetadata column = si.next(cd.column());
            assert column != null : cd.column().toString();

            if (cd.column().isSimple())
            {
                MSimpleCell persistentCell = Cell.cellWriter.writeCelltoMemory((Cell) cd, column, pkLiveness, mHeader);

                ((PersistentRow) mRow).addPersistentCell(domainData.pcm.get(column.toString(), PersistentString.class).getPColumnName(), persistentCell);
            }
            else
            {
                MComplexCell persistentComplexCell = writeComplexColumnToMemory((ComplexColumnData) cd, column, (flags & HAS_COMPLEX_DELETION) != 0,
                                                                                pkLiveness, mHeader);
                ((PersistentRow) mRow).addPersistentCell(domainData.pcm.get(column.toString(), PersistentString.class).getPColumnName(), persistentComplexCell);
            }
        }
    }

    private MComplexCell writeComplexColumnToMemory(ComplexColumnData data, ColumnMetadata column, boolean hasComplexDeletion,
                                                    LivenessInfo rowLiveness, MHeader header)
    throws IOException
    {
        MDeletionTime deletionTime = null;
        if (hasComplexDeletion)
        {
            deletionTime = MDeletionTime.getInstance(data.complexDeletion().markedForDeleteAt(),
                                                     data.complexDeletion().localDeletionTime());
        }
        MComplexCell persistentComplexCell = MComplexCell.getInstance(MUtils.getMDataType(mHeader.getType(column)),
                                                                      hasComplexDeletion, deletionTime);
        assert persistentComplexCell != null : "complex cell is null";
        for (Cell cell : data)
        {
            MSimpleCell persistentCell = Cell.cellWriter.writeCelltoMemory(cell, column, rowLiveness, header);
            persistentComplexCell.addCell(persistentCell);
        }
        return persistentComplexCell;
    }
}

class MiscDomainData
{
    public List<String> clusteringColNames;
    public MCFSortedMap cfMap;
    public MTable mTable;
    public MDecoratedKey partitionKey;
    public PersistentSIHashMap<PersistentString, PersistentColumnMetadata> pcm;

    public MiscDomainData()
    {
        this.clusteringColNames = new ArrayList<>();
    }
}
