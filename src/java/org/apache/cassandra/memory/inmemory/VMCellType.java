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

import org.apache.cassandra.memory.MCellType;
import org.apache.cassandra.memory.MConfig;

public enum VMCellType implements MCellType
{
    SIMPLE, COMPLEX;

    public static VMCellType getCellType(String cellType)
    {
        switch (cellType)
        {
            case MConfig.CELL_TYPE_SIMPLE:
                return SIMPLE;
            case MConfig.CELL_TYPE_COMPLEX:
                return COMPLEX;
            default:
                return SIMPLE;
        }
    }
}
