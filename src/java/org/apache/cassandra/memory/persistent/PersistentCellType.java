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

import lib.util.persistent.ObjectDirectory;
import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.types.IntField;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.memory.MCellType;
import org.apache.cassandra.memory.MConfig;

public final class PersistentCellType extends PersistentImmutableObject implements MCellType
{
    private static final IntField PM_CELL_TYPE_FIELD = new IntField();
    private static final ObjectType<PersistentCellType> TYPE = ObjectType.fromFields(PersistentCellType.class,
                                                                                     PM_CELL_TYPE_FIELD);
    private static Statics statics;

    // constructor
    private PersistentCellType(int cellType)
    {
        super(TYPE, (PersistentImmutableObject self) ->
        {
            self.initIntField(PM_CELL_TYPE_FIELD, cellType);
        });
    }

    // reconstructor
    private PersistentCellType(ObjectPointer<? extends PersistentCellType> pointer)
    {
        super(pointer);
    }

    public static PersistentCellType getCellType(String cellType)
    {
        return statics.getPMCellType(cellType);
    }

    private static final class Statics extends PersistentImmutableObject
    {
        private static final ObjectField<PersistentCellType> SIMPLE = new ObjectField<>();
        private static final ObjectField<PersistentCellType> COMPLEX = new ObjectField<>();
        private static final ObjectType<Statics> TYPE = ObjectType.fromFields(Statics.class, SIMPLE, COMPLEX);

        // constructor
        private Statics()
        {
            super(TYPE, (Statics self) ->
            {
                self.initObjectField(SIMPLE, new PersistentCellType(0));
                self.initObjectField(COMPLEX, new PersistentCellType(1));
            });
        }

        // reconstructor
        private Statics(ObjectPointer<Statics> pointer)
        {
            super(pointer);
        }

        private PersistentCellType getPMCellType(String cellType)
        {
            switch (cellType)
            {
                case MConfig.CELL_TYPE_SIMPLE:
                    return getObjectField(SIMPLE);
                case MConfig.CELL_TYPE_COMPLEX:
                    return getObjectField(COMPLEX);
                default:
                    return getObjectField(SIMPLE);
            }
        }
    }

    static
    {
        statics = ObjectDirectory.get("PM_CELL_TYPE", Statics.class);
        if (statics == null)
        {
            ObjectDirectory.put("PM_CELL_TYPE", statics = new Statics());
        }
    }
}
