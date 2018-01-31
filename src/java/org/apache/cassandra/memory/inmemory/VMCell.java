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

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.memory.MCellType;
import org.apache.cassandra.memory.MConfig;
import org.apache.cassandra.memory.MDataTypes;
import org.apache.cassandra.memory.MSimpleCell;

public final class VMCell extends VMCellBase implements MSimpleCell
{
    private byte flags;
    private long timestamp;
    private int localDeletionTime;
    private int ttl;
    private byte[] value;
    private byte[] cellPath;
    private MDataTypes dataType;

    // constructor
    private VMCell(MDataTypes dataType, byte flags, boolean setTimestamp, long timestamp,
                   boolean setLocalDeletionTime, int localDeletionTime, boolean setTTL, int ttl,
                   boolean setValue, byte[] value, boolean setCellPath, byte[] cellPath)
    {
        super(MCellType.getType(MConfig.CELL_TYPE_SIMPLE));
        this.localDeletionTime = Cell.NO_DELETION_TIME;
        this.ttl = Cell.NO_TTL;
        this.flags = flags;
        this.dataType = dataType;
        if (setTimestamp)
            this.timestamp = timestamp;
        if (setTTL)
            this.ttl = ttl;
        if (setLocalDeletionTime)
            this.localDeletionTime = localDeletionTime;
        if (setCellPath)
            this.cellPath = cellPath;
        if (setValue)
            this.value = value;
    }

    public static VMCell getInstance(MDataTypes dataType, byte flags, boolean setTimestamp, long timestamp,
                                     boolean setLocalDeletionTime, int localDeletionTime, boolean setTTL, int ttl,
                                     boolean setValue, byte[] value, boolean setCellPath, byte[] cellPath)
    {
        return new VMCell(dataType, flags, setTimestamp, timestamp, setLocalDeletionTime,
                          localDeletionTime, setTTL, ttl, setValue, value, setCellPath, cellPath);
    }

    // setters/getters
    @Override
    public int getLocalDeletionTime()
    {
        return localDeletionTime;
    }

    @Override
    public int getTtl()
    {
        return ttl;
    }

    @Override
    public byte[] getValue()
    {
        return value;
    }

    @Override
    public byte[] getCellPath()
    {
        return cellPath;
    }

    @Override
    public MDataTypes getDataType()
    {
        return dataType;
    }

    @Override
    public byte getFlags()
    {
        return flags;
    }

    @Override
    public long getTimestamp()
    {
        return timestamp;
    }
}
