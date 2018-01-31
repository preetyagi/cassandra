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

package org.apache.cassandra.memory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.schema.ColumnMetadata.Kind;

public class MUtils
{

    public static MDataTypes getMDataType(AbstractType<?> type)
    {
        if (type instanceof UUIDType)
            return MDataTypes.getType(MConfig.PM_UUID_TYPE);
        else if (type instanceof InetAddressType)
            return MDataTypes.getType(MConfig.PM_INET_ADDRESS_TYPE);
        else if (type instanceof TimeUUIDType)
            return MDataTypes.getType(MConfig.PM_TIMEUUID_TYPE);
        else if (type instanceof TimestampType)
            return MDataTypes.getType(MConfig.PM_TIMESTAMP_TYPE);
        else if (type instanceof DoubleType)
            return MDataTypes.getType(MConfig.PM_DOUBLE_TYPE);
        else if (type instanceof FloatType)
            return MDataTypes.getType(MConfig.PM_FLOAT_TYPE);
        else if (type instanceof BooleanType)
            return MDataTypes.getType(MConfig.PM_BOOLEAN_TYPE);
        else if (type instanceof UTF8Type)
            return MDataTypes.getType(MConfig.PM_UTF8_TYPE);
        else if (type instanceof BytesType)
            return MDataTypes.getType(MConfig.PM_BLOB_TYPE);
        else if (type instanceof Int32Type)
            return MDataTypes.getType(MConfig.PM_INTEGER_TYPE);
        else if (type instanceof LongType)
            return MDataTypes.getType(MConfig.PM_LONG_TYPE);
        else
            return MDataTypes.getType(MConfig.PM_INVALID_TYPE);
    }


    public static AbstractType<?> getTypeFromMData(MDataTypes mDataTypes)
    {
        if (mDataTypes == MDataTypes.getType(MConfig.PM_BOOLEAN_TYPE))
        {
            return BooleanType.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_TIMESTAMP_TYPE))
        {
            return TimestampType.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_INTEGER_TYPE))
        {
            return Int32Type.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_LONG_TYPE))
        {
            return LongType.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_STRING_TYPE))
        {
            return UTF8Type.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_DOUBLE_TYPE))
        {
            return DoubleType.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_FLOAT_TYPE))
        {
            return FloatType.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_TIMEUUID_TYPE))
        {
            return TimeUUIDType.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_BLOB_TYPE))
        {
            return BytesType.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_UTF8_TYPE))
        {
            return UTF8Type.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_UUID_TYPE))
        {
            return UUIDType.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_BIGINT_TYPE))
        {
            return IntegerType.instance;
        }
        else if (mDataTypes == MDataTypes.getType(MConfig.PM_INET_ADDRESS_TYPE))
        {
            return InetAddressType.instance;
        }
        else
            return null;
    }

    public static MColumnType getColumnType(Kind kind)
    {
        if (kind == Kind.PARTITION_KEY)
            return MColumnType.getType(MConfig.COL_TYPE_PK);
        else if (kind == Kind.CLUSTERING)
            return MColumnType.getType(MConfig.COL_TYPE_CK);
        else if (kind == Kind.REGULAR)
            return MColumnType.getType(MConfig.COL_TYPE_REGULAR);
        else if (kind == Kind.STATIC)
            return MColumnType.getType(MConfig.COL_TYPE_STATIC);
        else
            return MColumnType.getType(MConfig.COL_TYPE_UNKNOWN);
    }
}
