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
import lib.util.persistent.PersistentString;
import lib.util.persistent.types.BooleanField;
import lib.util.persistent.types.IntField;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import lib.util.persistent.types.StringField;
import org.apache.cassandra.memory.MColumnMetadata;
import org.apache.cassandra.memory.MColumnType;
import org.apache.cassandra.memory.MDataTypes;

public final class PersistentColumnMetadata extends PersistentImmutableObject implements MColumnMetadata
{
    private static final StringField COLUMN_NAME = new StringField();
    private static final ObjectField<PMDataTypes> DATA_TYPE = new ObjectField<>();
    private static final ObjectField<PersistentColumnType> COLUMN_TYPE = new ObjectField<>();
    private static final BooleanField IS_MULTI_CELL = new BooleanField();

    private static final ObjectType<PersistentColumnMetadata> TYPE = ObjectType.fromFields(PersistentColumnMetadata.class,
                                                                                           COLUMN_NAME,
                                                                                           DATA_TYPE,
                                                                                           COLUMN_TYPE,
                                                                                           IS_MULTI_CELL);

    // constructor
    public PersistentColumnMetadata(String columnName, MColumnType columnType, MDataTypes dataType, boolean flag)
    {
        super(TYPE, (PersistentImmutableObject self) ->
        {
            self.initObjectField(COLUMN_NAME, PersistentString.make(columnName));
            self.initObjectField(COLUMN_TYPE, (PersistentColumnType) columnType);
            self.initObjectField(DATA_TYPE, (PMDataTypes) dataType);
            self.initBooleanField(IS_MULTI_CELL, flag);
        });
    }

    public PersistentColumnMetadata(PersistentString columnName, MColumnType columnType, MDataTypes dataType, boolean flag)
    {
        super(TYPE, (PersistentImmutableObject self) ->
        {
            self.initObjectField(COLUMN_NAME, columnName);
            self.initObjectField(COLUMN_TYPE, (PersistentColumnType) columnType);
            self.initObjectField(DATA_TYPE, (PMDataTypes) dataType);
            self.initBooleanField(IS_MULTI_CELL, flag);
        });
    }

    // reconstructor
    private PersistentColumnMetadata(ObjectPointer<? extends PersistentColumnMetadata> pointer)
    {
        super(pointer);
    }

    @Override
    public String getColumnName()
    {
        return getObjectField(COLUMN_NAME).toString();
    }

    public PersistentString getPColumnName()
    {
        return getObjectField(COLUMN_NAME);
    }

    @Override
    public PMDataTypes getDataType()
    {
        return getObjectField(DATA_TYPE);
    }

    @Override
    public boolean getIsMultiCell()
    {
        return getBooleanField(IS_MULTI_CELL);
    }
}
