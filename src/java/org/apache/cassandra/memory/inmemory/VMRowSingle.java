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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MRowSingle;

// TODO: Figure out if VMRowSingle is really necessary
public final class VMRowSingle implements MRowSingle
{
    private List<MRow> rowList;

    // constructor
    private VMRowSingle()
    {
        rowList = new ArrayList<>();
    }

    public static VMRowSingle getInstance()
    {
        return new VMRowSingle();
    }

    @Override
    public Iterator<MRow> getRowIterator()
    {
        return rowList.iterator();
    }

    @Override
    public void add(MRow row)
    {
        MRow mRow = getRow();
        if (mRow == null)
        {
            rowList.add(row);
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
        if (rowList.size() == 0)
            return null;
        return rowList.get(0);
    }
}
