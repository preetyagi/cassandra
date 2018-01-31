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

package org.apache.cassandra.memory;

import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.memory.inmemory.VMRow;
import org.apache.cassandra.memory.persistent.PersistentRow;

public interface MRow
{
    public static MRow getInstance()
    {
        if (DatabaseDescriptor.isPersistentMemoryEnabled())
            return new PersistentRow();
        else if (DatabaseDescriptor.isInMemoryEnabled())
            return VMRow.getInstance();

        return null;
    }

    public MRowType getType();

    public void setType(MRowType type);

    public byte getFlags();

    public void setFlags(byte flags);

    public byte getExtendedFlags();

    public void setExtendedFlags(byte extendedFlags);

    public long getPrimaryKeyLivenessTimestamp();

    public void setPrimaryKeyLivenessTimestamp(long primaryKeyLivenessTimestamp);

    public int getPrimaryKeyTTL();

    public void setPrimaryKeyTTL(int primaryKeyTTL);

    public int getPkLocalExpirationTime();

    public void setPkLocalExpirationTime(int pkLocalExpirationTime);

    public MDeletionTime getDeletionTime();

    public void setDeletionTime(MDeletionTime deletionTime);

    public <T, U extends MCellBase> ConcurrentNavigableMap<T, U> getCells();

    public void addCell(String cellName, MCellBase cell);
}
