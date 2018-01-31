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
import org.apache.cassandra.memory.MConfig;
import org.apache.cassandra.memory.MRowType;

public class PersistentRowType extends PersistentImmutableObject implements MRowType
{

    private static final IntField PM_ROW_TYPE_FIELD = new IntField();
    private static final ObjectType<PersistentRowType> TYPE = ObjectType.fromFields(PersistentRowType.class,
                                                                                    PM_ROW_TYPE_FIELD);

    private static Statics statics;

    private PersistentRowType(int rowType)
    {
        super(TYPE, (PersistentImmutableObject self) ->
        {
            self.initIntField(PM_ROW_TYPE_FIELD, rowType);
        });
    }

    // reconstructor
    private PersistentRowType(ObjectPointer<? extends PersistentRowType> pointer)
    {
        super(pointer);
    }

    public static PersistentRowType getRowType(String rowType)
    {
        return statics.getPMRowType(rowType);
    }

    private static final class Statics extends PersistentImmutableObject
    {
        private static final ObjectField<PersistentRowType> REGULAR = new ObjectField<>();
        private static final ObjectField<PersistentRowType> STATIC = new ObjectField<>();
        private static final ObjectType<Statics> TYPE = ObjectType.fromFields(Statics.class, REGULAR, STATIC);

        // constructor
        private Statics()
        {
            super(TYPE, (Statics self) ->
            {
                self.initObjectField(REGULAR, new PersistentRowType(0));
                self.initObjectField(STATIC, new PersistentRowType(1));
            });
        }

        // reconstructor
        private Statics(ObjectPointer<? extends Statics> pointer)
        {
            super(pointer);
        }

        private PersistentRowType getPMRowType(String rowType)
        {
            switch (rowType)
            {
                case MConfig.ROW_TYPE_REGULAR:
                    return getObjectField(REGULAR);
                case MConfig.ROW_TYPE_STATIC:
                    return getObjectField(STATIC);
                default:
                    return getObjectField(REGULAR);
            }
        }
    }

    static
    {
        statics = ObjectDirectory.get("PM_ROW_TYPE", Statics.class);
        if (statics == null)
        {
            ObjectDirectory.put("PM_ROW_TYPE", statics = new Statics());
        }
    }
}
