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

import lib.util.persistent.ObjectPointer;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.memory.MDataTypes;
import org.apache.cassandra.memory.MListType;

public final class PMListType extends PMDataTypes implements MListType
{
    private static final ObjectField<PMDataTypes> ELEMENT_TYPE = new ObjectField<>();

    private static final ObjectType<PMListType> TYPE = PMDataTypes.TYPE.extendWith(PMListType.class, ELEMENT_TYPE);

    // constructor
    private PMListType(MDataTypes elementType)
    {
        super(TYPE, (PMListType self) ->
        {
            self.initObjectField(ELEMENT_TYPE, (PMDataTypes) elementType);
        });
    }

    // reconstructor
    private PMListType(ObjectPointer<? extends PMListType> pointer)
    {
        super(pointer);
    }

    public static PMListType getInstance(MDataTypes elementType)
    {
        return new PMListType(elementType);
    }

    @Override
    public MDataTypes getElementType()
    {
        return getObjectField(ELEMENT_TYPE);
    }

    @Override
    public int hashCode()
    {
        return getOrdinal(getObjectField(ELEMENT_TYPE));
    }

    @Override
    public boolean equals(Object obj)
    {
        // sanity checks
        if (obj == null)
            return false;
        else if (this == obj)
            return true;
        else if (getClass() != obj.getClass())
            return false;

        return (getElementType() == ((PMListType) obj).getElementType());
    }
}