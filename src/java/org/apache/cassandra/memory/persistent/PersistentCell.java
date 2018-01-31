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
import lib.util.persistent.PersistentImmutableByteArray;
import lib.util.persistent.types.ByteField;
import lib.util.persistent.types.FinalObjectField;
import lib.util.persistent.types.IntField;
import lib.util.persistent.types.LongField;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.memory.MCellType;
import org.apache.cassandra.memory.MConfig;
import org.apache.cassandra.memory.MDataTypes;
import org.apache.cassandra.memory.MSimpleCell;

public final class PersistentCell extends PersistentCellBase implements MSimpleCell
{
    private static final FinalObjectField<PersistentCellType> CELL_TYPE = new FinalObjectField<>();
    private static final ByteField FLAGS = new ByteField();
    private static final LongField TIMESTAMP = new LongField();
    private static final IntField LOCAL_DELETION_TIME = new IntField();
    private static final IntField TTL = new IntField();
    private static final ObjectField<PersistentImmutableByteArray> VALUE = new ObjectField<>();
    private static final ObjectField<PersistentImmutableByteArray> CELL_PATH = new ObjectField<>();
    private static final ObjectField<PMDataTypes> DATA_TYPE = new ObjectField<>();


    public static final ObjectType<PersistentCell> TYPE = ObjectType.fromFields(PersistentCell.class,
                                                                                CELL_TYPE,
                                                                                FLAGS,
                                                                                TIMESTAMP,
                                                                                LOCAL_DELETION_TIME,
                                                                                TTL,
                                                                                VALUE,
                                                                                CELL_PATH,
                                                                                DATA_TYPE);

    // constructor
    public PersistentCell(MDataTypes dataType, byte flags, boolean setTimestamp, long timestamp,
                          boolean setLocalDeletionTime, int localDeletionTime, boolean setTTL, int ttl, boolean setValue,
                          byte[] value, boolean setCellPath, byte[] cellPath)
    {
        super(TYPE, MCellType.getType(MConfig.CELL_TYPE_SIMPLE), (PersistentCell self) ->
        {
            self.initIntField(LOCAL_DELETION_TIME, Cell.NO_DELETION_TIME);
            self.initIntField(TTL, Cell.NO_TTL);
            self.initObjectField(DATA_TYPE, (PMDataTypes) dataType);
            self.initByteField(FLAGS, flags);
            if (setTimestamp)
                self.initLongField(TIMESTAMP, timestamp);
            if (setTTL)
                self.initIntField(TTL, ttl);
            if (setLocalDeletionTime)
                self.initIntField(LOCAL_DELETION_TIME, localDeletionTime);
            if (setCellPath)
                self.initObjectField(CELL_PATH, new PersistentImmutableByteArray(cellPath));
            if (setValue)
                self.initObjectField(VALUE, new PersistentImmutableByteArray(value));
        });
    }

    // reconstructor
    private PersistentCell(ObjectPointer<? extends PersistentCell> pointer)
    {
        super(pointer);
    }

    // setters/getters
    @Override
    public int getLocalDeletionTime()
    {
        return getIntField(LOCAL_DELETION_TIME);
    }

    @Override
    public int getTtl()
    {
        return getIntField(TTL);
    }

    @Override
    public byte[] getValue()
    {
        PersistentImmutableByteArray bArray = getObjectField(VALUE);
        return (bArray != null) ? bArray.toArray() : null;
    }

    @Override
    public byte[] getCellPath()
    {
        PersistentImmutableByteArray bArray = getObjectField(CELL_PATH);
        return (bArray != null) ? bArray.toArray() : null;
    }

    @Override
    public MDataTypes getDataType()
    {
        return getObjectField(DATA_TYPE);
    }

    @Override
    public byte getFlags()
    {
        return getByteField(FLAGS);
    }

    @Override
    public long getTimestamp()
    {
        return getLongField(TIMESTAMP);
    }
}
