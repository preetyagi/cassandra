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

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.memory.MDecoratedKey;
import org.apache.cassandra.memory.MToken;
import org.apache.cassandra.utils.FastByteOperations;

public final class VMDecoratedKey implements VMPartitionPosition, Comparable<VMDecoratedKey>, MDecoratedKey
{
    public SoftReference<DecoratedKey> decoratedKeySoftReference;
    MToken token;
    private ByteBuffer partitionKeyBuffer;

    // constructor
    public VMDecoratedKey(ByteBuffer partitionKeyBuffer, DecoratedKey decoratedKey, MToken token)
    {
        this.partitionKeyBuffer = partitionKeyBuffer;
        this.decoratedKeySoftReference = new SoftReference<>(decoratedKey);
        this.token = token;
    }

    public static VMDecoratedKey getInstance(byte[] partitionKeyBuffer, DecoratedKey decoratedKey, MToken token)
    {
        return new VMDecoratedKey(ByteBuffer.wrap(partitionKeyBuffer), decoratedKey, token);
    }

    private ByteBuffer getKeyInternal()
    {
        return partitionKeyBuffer;
    }

    @Override
    public byte[] getKey()
    {
        return partitionKeyBuffer.array();
    }

    @Override
    public MToken getToken()
    {
        return token;
    }

    @Override
    public int compareTo(VMDecoratedKey pos)
    {
        if (this == pos)
            return 0;

        byte[] b1 = getKey();
        byte[] b2 = pos.getKey();
        int cmp = getToken().compareTo(pos.getToken());
        return cmp == 0 ? FastByteOperations.compareUnsigned(b1, 0, b1.length, b2, 0, b2.length) : cmp;
    }

    @Override
    public int hashCode()
    {
        return (getKeyInternal().hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        // sanity checks
        if (obj == null)
            return false;
        else if (this == obj)
            return true;
        else if (this.getClass() != obj.getClass())
            return false;

        return compareTo((VMDecoratedKey) obj) == 0 ? true : false;
    }
}

