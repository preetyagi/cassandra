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

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.memory.MTable;
import org.apache.cassandra.memory.MTableMetadata;
import org.apache.cassandra.memory.MTablesManager;
import org.apache.cassandra.schema.TableMetadata;

public final class VMTablesManager implements MTablesManager
{

    public ConcurrentHashMap<UUID, VMTable> mtableManager;


    // constructor
    private VMTablesManager()
    {
        mtableManager = new ConcurrentHashMap<>();
    }

    public static VMTablesManager getInstance()
    {
        return new VMTablesManager();
    }

    private ConcurrentHashMap<UUID, VMTable> getVMTablesManager()
    {
        return mtableManager;
    }

    // Check if a PMTable instance exists for a given table id
    @Override
    public boolean doesMTableExist(UUID tableId)
    {
        return getVMTablesManager().containsKey(tableId);
    }

    // Create a PMTable for a given table id if one doesn't exist already. Ideally, there should be
    // not attempt to create a table again. Once a table is created for a given id, it can only be
    // deleted and then inserted again.
    @Override
    public boolean createMTableIfAbsent(UUID tableId, boolean isClusteringKeyAvailable,
                                         MTableMetadata tableMetadata, TableMetadata tempMetadata)
    {
        VMTable VMTable = getVMTablesManager().putIfAbsent(tableId, (VMTable) MTable.getInstance(tableMetadata,
                                                                                                 isClusteringKeyAvailable,
                                                                                                 tempMetadata));
        if (VMTable == null)
            return true;
        else
            return false;
    }

    // Return VMTable instance for a given table id
    @Override
    public VMTable getMTable(UUID tableId)
    {
        return getVMTablesManager().get(tableId);
    }
}
