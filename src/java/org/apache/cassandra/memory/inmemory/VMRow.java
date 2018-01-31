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

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.memory.MCellBase;
import org.apache.cassandra.memory.MConfig;
import org.apache.cassandra.memory.MDeletionTime;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MRowType;

public final class VMRow implements MRow
{
    private MRowType type;
    private byte flags;
    private byte extendedFlags;
    private long primaryKeyLivenessTimestamp;
    private int primaryKeyTTL;
    private int pkLocalExpirationTime;
    private MDeletionTime deletionTime;
    private ConcurrentSkipListMap<String, VMCellBase> cells;

    // constructor
    private VMRow()
    {
        type = VMRowType.getRowType(MConfig.ROW_TYPE_REGULAR);
        cells = new ConcurrentSkipListMap<>();
    }

    public static VMRow getInstance()
    {
        return new VMRow();
    }

    @Override
    public MRowType getType()
    {
        return type;
    }

    @Override
    public void setType(MRowType type)
    {
        this.type = type;
    }

    @Override
    public byte getFlags()
    {
        return flags;
    }

    @Override
    public void setFlags(byte flags)
    {
        this.flags = flags;
    }

    @Override
    public byte getExtendedFlags()
    {
        return extendedFlags;
    }

    @Override
    public void setExtendedFlags(byte extendedFlags)
    {
        this.extendedFlags = extendedFlags;
    }

    @Override
    public long getPrimaryKeyLivenessTimestamp()
    {
        return primaryKeyLivenessTimestamp;
    }

    @Override
    public void setPrimaryKeyLivenessTimestamp(long primaryKeyLivenessTimestamp)
    {
        this.primaryKeyLivenessTimestamp = primaryKeyLivenessTimestamp;
    }

    @Override
    public int getPrimaryKeyTTL()
    {
        return primaryKeyTTL;
    }

    @Override
    public void setPrimaryKeyTTL(int primaryKeyTTL)
    {
        this.primaryKeyTTL = primaryKeyTTL;
    }

    @Override
    public int getPkLocalExpirationTime()
    {
        return pkLocalExpirationTime;
    }

    @Override
    public void setPkLocalExpirationTime(int pkLocalExpirationTime)
    {
        this.pkLocalExpirationTime = pkLocalExpirationTime;
    }

    @Override
    public MDeletionTime getDeletionTime()
    {
        return deletionTime;
    }

    @Override
    public void setDeletionTime(MDeletionTime deletionTime)
    {
        this.deletionTime = deletionTime;
    }

    @Override
    public ConcurrentSkipListMap<String, VMCellBase> getCells()
    {
        return cells;
    }

    @Override
    public void addCell(String cellName, MCellBase cell)
    {
        getCells().put(cellName, (VMCellBase) cell);
    }
}
