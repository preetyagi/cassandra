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

import java.util.Iterator;

import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentArrayList;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.types.FinalObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MRowSingle;

public final class PersistentRowSingle extends PersistentImmutableObject implements MRowSingle
{
    private static final FinalObjectField<PersistentArrayList<PersistentRow>> ROW_LIST = new FinalObjectField<>();

    private static final ObjectType<PersistentRowSingle> TYPE = ObjectType.fromFields(PersistentRowSingle.class,
                                                                                      ROW_LIST);

    // constructor
    public PersistentRowSingle()
    {
        super(TYPE, (PersistentImmutableObject self) ->
        {
            self.initObjectField(ROW_LIST, new PersistentArrayList<>());
        });
    }

    // reconstructor
    private PersistentRowSingle(ObjectPointer<? extends PersistentRowSingle> pointer)
    {
        super(pointer);
    }

    private PersistentArrayList<PersistentRow> getRowList()
    {
        return getObjectField(ROW_LIST);
    }

    @Override
    public Iterator<PersistentRow> getRowIterator()
    {
        return getRowList().iterator();
    }

    @Override
    public void add(MRow row)
    {
        MRow mRow = getRow();
        if (mRow == null)
        {
            getRowList().add((PersistentRow) row);
        }
        else
        {
            mRow.getCells().putAll(row.getCells());
            mRow.setType(row.getType());
            mRow.setFlags(row.getFlags());
            mRow.setExtendedFlags(row.getExtendedFlags());
            mRow.setPrimaryKeyLivenessTimestamp(row.getPrimaryKeyLivenessTimestamp());
            mRow.setPkLocalExpirationTime(row.getPkLocalExpirationTime());
            if (row.getDeletionTime() != null)
                mRow.setDeletionTime(row.getDeletionTime());
        }
    }

    private MRow getRow()
    {
        if (getObjectField(ROW_LIST).size() == 0)
            return null;
        return getRowList().get(0);
    }
}
