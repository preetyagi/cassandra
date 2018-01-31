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
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.memory.MCellBase;
import org.apache.cassandra.memory.MCellType;

public abstract class PersistentCellBase extends PersistentImmutableObject implements MCellBase
{
    private static final ObjectField<PersistentCellType> PM_CELL_BASE_OBJ_FIELD = new ObjectField<>();
    public static final ObjectType<PersistentCellBase> TYPE = ObjectType.fromFields(PersistentCellBase.class,
                                                                                    PM_CELL_BASE_OBJ_FIELD);

    // subclassing constructor
    protected <T extends PersistentCellBase> PersistentCellBase(ObjectType<T> type, MCellType cellType,
                                                                java.util.function.Consumer<T> initializer)
    {
        super(type, (PersistentCellBase self) ->
        {
            self.initObjectField(PM_CELL_BASE_OBJ_FIELD, (PersistentCellType) cellType);
            if (initializer != null) initializer.accept((T) self);
        });
    }

    // reconstructor
    protected PersistentCellBase(ObjectPointer<? extends PersistentCellBase> pointer)
    {
        super(pointer);
    }

    @Override
    public MCellType getType()
    {
        return getObjectField(PM_CELL_BASE_OBJ_FIELD);
    }
}
