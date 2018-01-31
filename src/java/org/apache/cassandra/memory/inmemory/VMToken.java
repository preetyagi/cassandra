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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.memory.MToken;

public abstract class VMToken implements MToken
{
    public static <T> VMToken getInstance(T token)
    {
        if (DatabaseDescriptor.getPartitionerName().equalsIgnoreCase("org.apache.cassandra.dht.Murmur3Partitioner"))
            return new VMLongToken((Long) token);
        else if (DatabaseDescriptor.getPartitionerName().equalsIgnoreCase("org.apache.cassandra.dht.ByteOrderedPartitioner"))
            return new VMBytesToken((byte[]) token);
        return null;
    }

    public abstract <T> T getToken();

    public abstract <T> int compareTo(T token);
}
