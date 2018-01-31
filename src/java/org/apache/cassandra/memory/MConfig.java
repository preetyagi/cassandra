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

// This class contains all global definitions used across all implementations
public final class MConfig
{
    public static final String ROOT_CASSANDRA_DB = "ROOT_CASSANDRA_DB";

    public static final String CELL_TYPE_SIMPLE = "CELL_TYPE_SIMPLE";
    public static final String CELL_TYPE_COMPLEX = "CELL_TYPE_COMPLEX";

    public static final String COL_TYPE_UNKNOWN = "COL_TYPE_UNKNOWN";
    public static final String COL_TYPE_PK = "COL_TYPE_PK";
    public static final String COL_TYPE_CK = "COL_TYPE_CK";
    public static final String COL_TYPE_REGULAR = "COL_TYPE_REGULAR";
    public static final String COL_TYPE_STATIC = "COL_TYPE_STATIC";

    public static final String ROW_TYPE_REGULAR = "ROW_TYPE_REGULAR";
    public static final String ROW_TYPE_STATIC = "ROW_TYPE_STATIC";

    public static final String PM_INVALID_TYPE = "PM_INVALID_TYPE";
    public static final String PM_BOOLEAN_TYPE = "PM_BOOLEAN_TYPE";
    public static final String PM_TIMESTAMP_TYPE = "PM_TIMESTAMP_TYPE";
    public static final String PM_INTEGER_TYPE = "PM_INTEGER_TYPE";
    public static final String PM_LONG_TYPE = "PM_LONG_TYPE";
    public static final String PM_STRING_TYPE = "PM_STRING_TYPE";
    public static final String PM_DOUBLE_TYPE = "PM_DOUBLE_TYPE";
    public static final String PM_FLOAT_TYPE = "PM_FLOAT_TYPE";
    public static final String PM_TIMEUUID_TYPE = "PM_TIMEUUID_TYPE";
    public static final String PM_BLOB_TYPE = "PM_BLOB_TYPE";
    public static final String PM_UTF8_TYPE = "PM_UTF8_TYPE";
    public static final String PM_UUID_TYPE = "PM_UUID_TYPE";
    public static final String PM_BIGINT_TYPE = "PM_BIGINT_TYPE";
    public static final String PM_INET_ADDRESS_TYPE = "PM_INET_ADDRESS_TYPE";
}
