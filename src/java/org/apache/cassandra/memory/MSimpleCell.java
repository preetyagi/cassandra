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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.memory.inmemory.VMCell;
import org.apache.cassandra.memory.persistent.PersistentCell;

public interface MSimpleCell extends MCellBase
{
    public static MSimpleCell getInstance(MDataTypes dataType, byte flags, boolean setTimestamp, long timestamp,
                                          boolean setLocalDeletionTime, int localDeletionTime, boolean setTTL, int ttl,
                                          boolean setValue, byte[] value, boolean setCellPath, byte[] cellPath)
    {
        if (DatabaseDescriptor.isPersistentMemoryEnabled())
            return new PersistentCell(dataType, flags, setTimestamp, timestamp, setLocalDeletionTime,
                                              localDeletionTime, setTTL, ttl, setValue, value, setCellPath, cellPath);
        else if (DatabaseDescriptor.isInMemoryEnabled())
            return VMCell.getInstance(dataType, flags, setTimestamp, timestamp, setLocalDeletionTime,
                                      localDeletionTime, setTTL, ttl, setValue, value, setCellPath, cellPath);

        return null;
    }

    public int getLocalDeletionTime();

    //  public void setLocalDeletionTime(int localDeletionTime);

    public int getTtl();

    // public void setTtl(int ttl);

    public byte[] getValue();

    // public void setValue(byte[] value);

    public byte[] getCellPath();

    //  public void setCellPath(byte[] cellPath);

    public MDataTypes getDataType();

    // public void setDataType(MDataTypes type);

    public byte getFlags();

    //  public void setFlags(byte flags);

    public long getTimestamp();

    // public void setTimestamp(long timestamp);
}
