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
import org.apache.cassandra.memory.MDataTypes;

public class PMDataTypes extends PersistentImmutableObject implements MDataTypes
{

    private static final IntField PM_DATA_TYPE_FIELD = new IntField();

    public static final ObjectType<PMDataTypes> TYPE = ObjectType.fromFields(PMDataTypes.class,
                                                                             PM_DATA_TYPE_FIELD);
    private static Statics statics;

    // constructor
    protected PMDataTypes(int dataType)
    {
        super(TYPE, (PersistentImmutableObject self) ->
        {
            self.initIntField(PM_DATA_TYPE_FIELD, dataType);
        });
    }

    // subclassing constructor
    protected <T extends PMDataTypes> PMDataTypes(ObjectType<T> type, java.util.function.Consumer<T> initializer)
    {
        super(type, (PMDataTypes self) ->
        {
            if (initializer != null) initializer.accept((T) self);
        });
    }

    // reconstructor
    protected PMDataTypes(ObjectPointer<? extends PMDataTypes> pointer)
    {
        super(pointer);
    }

    public static MDataTypes getDataType(String dataType)
    {
        return statics.getPMDataType(dataType);
    }

    protected static int getOrdinal(PMDataTypes dataType)
    {
        return statics.getOrdinal(dataType);
    }

    private static final class Statics extends PersistentImmutableObject
    {
        private static final ObjectField<PMDataTypes> PM_INVALID_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_BOOLEAN_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_TIMESTAMP_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_INTEGER_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_LONG_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_STRING_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_DOUBLE_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_FLOAT_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_TIMEUUID_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_BLOB_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_UTF8_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_UUID_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_BIGINT_TYPE = new ObjectField<>();
        private static final ObjectField<PMDataTypes> PM_INET_ADDRESS_TYPE = new ObjectField<>();

        private static final int INVALID_TYPE = 0;
        private static final int BOOLEAN_TYPE = 1;
        private static final int TIMESTAMP_TYPE = 2;
        private static final int INTEGER_TYPE = 3;
        private static final int LONG_TYPE = 4;
        private static final int STRING_TYPE = 5;
        private static final int DOUBLE_TYPE = 6;
        private static final int FLOAT_TYPE = 7;
        private static final int TIMEUUID_TYPE = 8;
        private static final int BLOB_TYPE = 9;
        private static final int UTF8_TYPE = 10;
        private static final int UUID_TYPE = 11;
        private static final int BIGINT_TYPE = 12;
        private static final int INET_ADDRESS_TYPE = 13;


        private static final ObjectType<Statics> TYPE = ObjectType.fromFields(Statics.class,
                                                                              PM_INVALID_TYPE,
                                                                              PM_BOOLEAN_TYPE,
                                                                              PM_TIMESTAMP_TYPE,
                                                                              PM_INTEGER_TYPE,
                                                                              PM_LONG_TYPE,
                                                                              PM_STRING_TYPE,
                                                                              PM_DOUBLE_TYPE,
                                                                              PM_FLOAT_TYPE,
                                                                              PM_TIMEUUID_TYPE,
                                                                              PM_BLOB_TYPE,
                                                                              PM_UTF8_TYPE,
                                                                              PM_UUID_TYPE,
                                                                              PM_BIGINT_TYPE,
                                                                              PM_INET_ADDRESS_TYPE);

        private Statics()
        {
            super(TYPE, (Statics self) ->
            {
                self.initObjectField(PM_INVALID_TYPE, new PMDataTypes(INVALID_TYPE));
                self.initObjectField(PM_BOOLEAN_TYPE, new PMDataTypes(BOOLEAN_TYPE));
                self.initObjectField(PM_TIMESTAMP_TYPE, new PMDataTypes(TIMESTAMP_TYPE));
                self.initObjectField(PM_INTEGER_TYPE, new PMDataTypes(INTEGER_TYPE));
                self.initObjectField(PM_LONG_TYPE, new PMDataTypes(LONG_TYPE));
                self.initObjectField(PM_STRING_TYPE, new PMDataTypes(STRING_TYPE));
                self.initObjectField(PM_DOUBLE_TYPE, new PMDataTypes(DOUBLE_TYPE));
                self.initObjectField(PM_FLOAT_TYPE, new PMDataTypes(FLOAT_TYPE));
                self.initObjectField(PM_TIMEUUID_TYPE, new PMDataTypes(TIMEUUID_TYPE));
                self.initObjectField(PM_BLOB_TYPE, new PMDataTypes(BLOB_TYPE));
                self.initObjectField(PM_UTF8_TYPE, new PMDataTypes(UTF8_TYPE));
                self.initObjectField(PM_UUID_TYPE, new PMDataTypes(UUID_TYPE));
                self.initObjectField(PM_BIGINT_TYPE, new PMDataTypes(BIGINT_TYPE));
                self.initObjectField(PM_INET_ADDRESS_TYPE, new PMDataTypes(INET_ADDRESS_TYPE));
            });
        }

        // reconstructor
        private Statics(ObjectPointer<Statics> pointer)
        {
            super(pointer);
        }

        private PMDataTypes getPMDataType(String dataType)
        {
            switch (dataType)
            {
                case MConfig.PM_BOOLEAN_TYPE:
                    return getObjectField(PM_BOOLEAN_TYPE);
                case MConfig.PM_TIMESTAMP_TYPE:
                    return getObjectField(PM_TIMESTAMP_TYPE);
                case MConfig.PM_INTEGER_TYPE:
                    return getObjectField(PM_INTEGER_TYPE);
                case MConfig.PM_LONG_TYPE:
                    return getObjectField(PM_LONG_TYPE);
                case MConfig.PM_STRING_TYPE:
                    return getObjectField(PM_STRING_TYPE);
                case MConfig.PM_DOUBLE_TYPE:
                    return getObjectField(PM_DOUBLE_TYPE);
                case MConfig.PM_FLOAT_TYPE:
                    return getObjectField(PM_FLOAT_TYPE);
                case MConfig.PM_TIMEUUID_TYPE:
                    return getObjectField(PM_TIMEUUID_TYPE);
                case MConfig.PM_BLOB_TYPE:
                    return getObjectField(PM_BLOB_TYPE);
                case MConfig.PM_UTF8_TYPE:
                    return getObjectField(PM_UTF8_TYPE);
                case MConfig.PM_UUID_TYPE:
                    return getObjectField(PM_UUID_TYPE);
                case MConfig.PM_BIGINT_TYPE:
                    return getObjectField(PM_BIGINT_TYPE);
                case MConfig.PM_INET_ADDRESS_TYPE:
                    return getObjectField(PM_INET_ADDRESS_TYPE);
                default:
                    return getObjectField(PM_INVALID_TYPE);
            }
        }

        private int getOrdinal(PMDataTypes dataType)
        {
            if (dataType == getObjectField(PM_BOOLEAN_TYPE))
                return BOOLEAN_TYPE;
            else if (dataType == getObjectField(PM_TIMESTAMP_TYPE))
                return TIMESTAMP_TYPE;
            else if (dataType == getObjectField(PM_INTEGER_TYPE))
                return INTEGER_TYPE;
            else if (dataType == getObjectField(PM_LONG_TYPE))
                return LONG_TYPE;
            else if (dataType == getObjectField(PM_STRING_TYPE))
                return STRING_TYPE;
            else if (dataType == getObjectField(PM_DOUBLE_TYPE))
                return DOUBLE_TYPE;
            else if (dataType == getObjectField(PM_FLOAT_TYPE))
                return FLOAT_TYPE;
            else if (dataType == getObjectField(PM_TIMEUUID_TYPE))
                return TIMEUUID_TYPE;
            else if (dataType == getObjectField(PM_BLOB_TYPE))
                return BLOB_TYPE;
            else if (dataType == getObjectField(PM_UTF8_TYPE))
                return UTF8_TYPE;
            else if (dataType == getObjectField(PM_UUID_TYPE))
                return UUID_TYPE;
            else if (dataType == getObjectField(PM_BIGINT_TYPE))
                return BIGINT_TYPE;
            else if (dataType == getObjectField(PM_INET_ADDRESS_TYPE))
                return INET_ADDRESS_TYPE;
            else
                return INVALID_TYPE;
        }
    }

    static
    {
        statics = ObjectDirectory.get("PM_DATATYPE", Statics.class);
        if (statics == null)
        {
            ObjectDirectory.put("PM_DATATYPE", statics = new Statics());
        }
    }
}
