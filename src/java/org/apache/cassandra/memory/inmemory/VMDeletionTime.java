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

import org.apache.cassandra.memory.MDeletionTime;
import org.apache.cassandra.memory.MSimpleCell;

public final class VMDeletionTime implements Comparable<VMDeletionTime>, MDeletionTime
{
    private long markedForDeleteAt;
    private int localDeletionTime;

    // constructor
    private VMDeletionTime(long markedForDeleteAt, int localDeletionTime)
    {
        this.markedForDeleteAt = markedForDeleteAt;
        this.localDeletionTime = localDeletionTime;
    }

    public static VMDeletionTime getInstance(long markedForDeleteAt, int localDeletionTime)
    {
        return new VMDeletionTime(markedForDeleteAt, localDeletionTime);
    }

    // setters/getters
    @Override
    public long markedForDeleteAt()
    {
        return markedForDeleteAt;
    }

    @Override
    public int localDeletionTime()
    {
        return localDeletionTime;
    }

    // other helper/inherited methods
    public boolean isLive()
    {
        return markedForDeleteAt == Long.MIN_VALUE && localDeletionTime == Integer.MAX_VALUE;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof VMDeletionTime))
            return false;
        VMDeletionTime that = (VMDeletionTime) o;
        return markedForDeleteAt() == that.markedForDeleteAt() && localDeletionTime() == that.localDeletionTime();
    }

    @Override
    public final int hashCode()
    {
        // TODO: Uncomment hashCode method once ported to Cassandra source
        // The below hashCode() method is implemented in guava library
        // so won't compile in this project since we don't include that
        return (int) (markedForDeleteAt() + localDeletionTime());
        // return Objects.hashCode(markedForDeleteAt(), localDeletionTime());
    }

    @Override
    public String toString()
    {
        return String.format("deletedAt=%d, localDeletion=%d", markedForDeleteAt(), localDeletionTime());
    }

    @Override
    public int compareTo(VMDeletionTime dt)
    {
        if (markedForDeleteAt() < dt.markedForDeleteAt())
            return -1;
        else if (markedForDeleteAt() > dt.markedForDeleteAt())
            return 1;
        else if (localDeletionTime() < dt.localDeletionTime())
            return -1;
        else if (localDeletionTime() > dt.localDeletionTime())
            return 1;
        else
            return 0;
    }
}

