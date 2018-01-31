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

import org.apache.cassandra.memory.MConfig;
import org.apache.cassandra.memory.MDataTypes;

public enum VMDataTypes implements MDataTypes
{

    PM_INVALID_TYPE, PM_BOOLEAN_TYPE, PM_TIMESTAMP_TYPE, PM_INTEGER_TYPE, PM_LONG_TYPE, PM_STRING_TYPE,
    PM_DOUBLE_TYPE, PM_FLOAT_TYPE, PM_TIMEUUID_TYPE, PM_BLOB_TYPE, PM_UTF8_TYPE, PM_UUID_TYPE, PM_BIGINT_TYPE, PM_INET_ADDRESS_TYPE;

    public static MDataTypes getDataType(String dataType)
    {
        switch (dataType)
        {
            case MConfig.PM_BOOLEAN_TYPE:
                return PM_BOOLEAN_TYPE;
            case MConfig.PM_TIMESTAMP_TYPE:
                return PM_TIMESTAMP_TYPE;
            case MConfig.PM_INTEGER_TYPE:
                return PM_INTEGER_TYPE;
            case MConfig.PM_LONG_TYPE:
                return PM_LONG_TYPE;
            case MConfig.PM_STRING_TYPE:
                return PM_STRING_TYPE;
            case MConfig.PM_DOUBLE_TYPE:
                return PM_DOUBLE_TYPE;
            case MConfig.PM_FLOAT_TYPE:
                return PM_FLOAT_TYPE;
            case MConfig.PM_TIMEUUID_TYPE:
                return PM_TIMEUUID_TYPE;
            case MConfig.PM_BLOB_TYPE:
                return PM_BLOB_TYPE;
            case MConfig.PM_UTF8_TYPE:
                return PM_UTF8_TYPE;
            case MConfig.PM_UUID_TYPE:
                return PM_UUID_TYPE;
            case MConfig.PM_BIGINT_TYPE:
                return PM_BIGINT_TYPE;
            case MConfig.PM_INET_ADDRESS_TYPE:
                return PM_INET_ADDRESS_TYPE;
            default:
                return PM_INVALID_TYPE;
        }
    }
}
