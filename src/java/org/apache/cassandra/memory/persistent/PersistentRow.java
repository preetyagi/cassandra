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

import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentObject;
import lib.util.persistent.PersistentSkipListMap;
import lib.util.persistent.PersistentString;
import lib.util.persistent.types.ByteField;
import lib.util.persistent.types.IntField;
import lib.util.persistent.types.LongField;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.memory.MCellBase;
import org.apache.cassandra.memory.MConfig;
import org.apache.cassandra.memory.MDeletionTime;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MRowType;

public final class PersistentRow extends PersistentObject implements MRow
{
    private static final ObjectField<PersistentRowType> ROW_TYPE = new ObjectField<>();
    private static final ByteField FLAGS = new ByteField();
    private static final ByteField EXTENDED_FLAGS = new ByteField();
    private static final LongField PK_LIVENESS_TIMESTAMP = new LongField();
    private static final IntField PRIMARY_KEY_TTL = new IntField();
    private static final IntField PK_LOCAL_EXPIRATION_TIME = new IntField();
    private static final ObjectField<PersistentDeletionTime> DELETION_TIME = new ObjectField<>();
    private static final ObjectField<PersistentSkipListMap<PersistentString, PersistentCellBase>> CELLS = new
                                                                                                          ObjectField<>();
    private static final ObjectType<PersistentRow> TYPE = ObjectType.fromFields(PersistentRow.class, ROW_TYPE, FLAGS,
                                                                                EXTENDED_FLAGS, PK_LIVENESS_TIMESTAMP, PRIMARY_KEY_TTL, PK_LOCAL_EXPIRATION_TIME, DELETION_TIME, CELLS);

    // constructor
    public PersistentRow()
    {
        super(TYPE);
        setObjectField(ROW_TYPE, (PersistentRowType) MRowType.getType(MConfig.ROW_TYPE_REGULAR));
        setObjectField(CELLS, new PersistentSkipListMap<>());
    }

    // reconstructor
    private PersistentRow(ObjectPointer<? extends PersistentRow> pointer)
    {
        super(pointer);
    }

    @Override
    public PersistentRowType getType()
    {
        return getObjectField(ROW_TYPE);
    }

    @Override
    public void setType(MRowType type)
    {
        setObjectField(ROW_TYPE, (PersistentRowType) type);
    }

    @Override
    public byte getFlags()
    {
        return getByteField(FLAGS);
    }

    @Override
    public void setFlags(byte flags)
    {
        setByteField(FLAGS, flags);
    }

    @Override
    public byte getExtendedFlags()
    {
        return getByteField(EXTENDED_FLAGS);
    }

    @Override
    public void setExtendedFlags(byte extendedFlags)
    {
        setByteField(EXTENDED_FLAGS, extendedFlags);
    }

    @Override
    public long getPrimaryKeyLivenessTimestamp()
    {
        return getLongField(PK_LIVENESS_TIMESTAMP);
    }

    @Override
    public void setPrimaryKeyLivenessTimestamp(long primaryKeyLivenessTimestamp)
    {
        setLongField(PK_LIVENESS_TIMESTAMP, primaryKeyLivenessTimestamp);
    }

    @Override
    public int getPrimaryKeyTTL()
    {
        return getIntField(PRIMARY_KEY_TTL);
    }

    @Override
    public void setPrimaryKeyTTL(int primaryKeyTTL)
    {
        setIntField(PRIMARY_KEY_TTL, primaryKeyTTL);
    }

    @Override
    public int getPkLocalExpirationTime()
    {
        return getIntField(PK_LOCAL_EXPIRATION_TIME);
    }

    @Override
    public void setPkLocalExpirationTime(int pkLocalExpirationTime)
    {
        setIntField(PK_LOCAL_EXPIRATION_TIME, pkLocalExpirationTime);
    }

    @Override
    public MDeletionTime getDeletionTime()
    {
        return getObjectField(DELETION_TIME);
    }

    @Override
    public void setDeletionTime(MDeletionTime deletionTime)
    {
        setObjectField(DELETION_TIME, (PersistentDeletionTime) deletionTime);
    }

    @Override
    public PersistentSkipListMap<PersistentString, PersistentCellBase> getCells()
    {
        return getObjectField(CELLS);
    }

    public void addPersistentCell(PersistentString cellName, MCellBase cell)
    {
        getCells().put(cellName, (PersistentCellBase) cell);
    }

    @Override
    public void addCell(String cellName, MCellBase cell)
    {
        getCells().put(PersistentString.make(cellName), (PersistentCellBase) cell);
    }
}
