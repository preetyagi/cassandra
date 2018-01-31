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
import org.apache.cassandra.memory.MColumnType;
import org.apache.cassandra.memory.MConfig;

public final class PersistentColumnType extends PersistentImmutableObject implements MColumnType
{
    private static final IntField PM_COLUMN_TYPE = new IntField();
    private static final ObjectType<PersistentColumnType> TYPE = ObjectType.fromFields(PersistentColumnType.class,
                                                                                       PM_COLUMN_TYPE);

    private static Statics statics;

    // constructor
    private PersistentColumnType(int colType)
    {
        super(TYPE, (PersistentImmutableObject self) ->
        {
            self.initIntField(PM_COLUMN_TYPE, colType);
        });
    }

    // reconstructor
    private PersistentColumnType(ObjectPointer<? extends PersistentColumnType> pointer)
    {
        super(pointer);
    }

    public static PersistentColumnType getColumnType(String colType)
    {
        return statics.getPMColumnType(colType);
    }

    private static final class Statics extends PersistentImmutableObject
    {
        private static final ObjectField<PersistentColumnType> UNKNOWN = new ObjectField<>();
        private static final ObjectField<PersistentColumnType> PARTITION_KEY = new ObjectField<>();
        private static final ObjectField<PersistentColumnType> CLUSTERING_KEY = new ObjectField<>();
        private static final ObjectField<PersistentColumnType> REGULAR = new ObjectField<>();
        private static final ObjectField<PersistentColumnType> STATIC = new ObjectField<>();
        private static final ObjectType<Statics> TYPE = ObjectType.fromFields(Statics.class, UNKNOWN, PARTITION_KEY,
                                                                              CLUSTERING_KEY, REGULAR, STATIC);

        // constructor
        private Statics()
        {
            super(TYPE, (Statics self) ->
            {
                self.initObjectField(UNKNOWN, new PersistentColumnType(0));
                self.initObjectField(PARTITION_KEY, new PersistentColumnType(1));
                self.initObjectField(CLUSTERING_KEY, new PersistentColumnType(2));
                self.initObjectField(REGULAR, new PersistentColumnType(3));
                self.initObjectField(STATIC, new PersistentColumnType(4));
            });
        }

        // reconstructor
        private Statics(ObjectPointer<? extends PersistentColumnType> pointer)
        {
            super(pointer);
        }

        private PersistentColumnType getPMColumnType(String colType)
        {
            switch (colType)
            {
                case MConfig.COL_TYPE_PK:
                    return getObjectField(PARTITION_KEY);
                case MConfig.COL_TYPE_CK:
                    return getObjectField(CLUSTERING_KEY);
                case MConfig.COL_TYPE_REGULAR:
                    return getObjectField(REGULAR);
                case MConfig.COL_TYPE_STATIC:
                    return getObjectField(STATIC);
                default:
                    return getObjectField(UNKNOWN);
            }
        }
    }

    static
    {
        statics = ObjectDirectory.get("PM_COLUMN_TYPE", Statics.class);
        if (statics == null)
        {
            ObjectDirectory.put("PM_COLUMN_TYPE", statics = new Statics());
        }
    }
}

