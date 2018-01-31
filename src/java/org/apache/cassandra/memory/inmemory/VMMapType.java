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

import org.apache.cassandra.memory.MDataTypes;
import org.apache.cassandra.memory.MMapType;

public final class VMMapType implements MMapType
{
    VMDataTypes keyType;
    VMDataTypes valueType;

    // constructor
    private VMMapType(VMDataTypes keyType, VMDataTypes valType)
    {
        this.keyType = keyType;
        this.valueType = valType;
    }

    public static VMMapType getInstance(MDataTypes keyType, MDataTypes valType)
    {
        return new VMMapType((VMDataTypes) keyType, (VMDataTypes) valType);
    }

    @Override
    public MDataTypes getKeyType()
    {
        return keyType;
    }

    @Override
    public MDataTypes getValType()
    {
        return valueType;
    }

    @Override
    public int hashCode()
    {
        return keyType.ordinal() + valueType.ordinal();
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

        return (this.getKeyType() == ((VMMapType) obj).getKeyType()) &&
               (this.getValType() == ((VMMapType) obj).getValType());
    }
}