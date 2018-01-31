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
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.memory.MColumnMetadata;
import org.apache.cassandra.memory.MTableMetadata;

public final class VMTableMetadata implements MTableMetadata
{
    private String keyspace;
    private String tableName;
    private UUID tableId;

    // Column information
    private List<MColumnMetadata> partitionKey;
    private List<MColumnMetadata> clusteringKey;
    private List<MColumnMetadata> regularAndStaticColumns;

    // constructor
    public VMTableMetadata(String keyspace, String tableName, UUID tableId)
    {
        this.keyspace = keyspace;
        this.tableName = tableName;
        this.tableId = tableId;
        this.partitionKey = new ArrayList<>();
        this.clusteringKey = new ArrayList<>();
        this.regularAndStaticColumns = new ArrayList<>();
    }

    @Override
    public String getKeyspace()
    {
        return keyspace;
    }

    @Override
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public UUID getTableId()
    {
        return tableId;
    }

    @Override
    public List<MColumnMetadata> getPartitionKey()
    {
        return partitionKey;
    }

    @Override
    public void addPartitionKey(MColumnMetadata columnMetadata)
    {
        partitionKey.add(columnMetadata);
    }

    @Override
    public List<MColumnMetadata> getClusteringKey()
    {
        return clusteringKey;
    }

    @Override
    public void addClusteringKey(MColumnMetadata columnMetadata)
    {
        clusteringKey.add(columnMetadata);
    }
}
