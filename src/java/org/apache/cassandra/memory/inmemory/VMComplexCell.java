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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.memory.MCellType;
import org.apache.cassandra.memory.MComplexCell;
import org.apache.cassandra.memory.MConfig;
import org.apache.cassandra.memory.MDataTypes;
import org.apache.cassandra.memory.MDeletionTime;
import org.apache.cassandra.memory.MSimpleCell;

public final class VMComplexCell extends VMCellBase implements MComplexCell
{
    private MDeletionTime complexDeletionTime;
    private List<VMCell> cells;
    private MDataTypes complexDataType;

    // constructor
    private VMComplexCell(MDataTypes dataType)
    {
        super(MCellType.getType(MConfig.CELL_TYPE_COMPLEX));
        this.complexDataType = dataType;
        this.cells = new ArrayList<>();
    }

    public static VMComplexCell getInstance(MDataTypes dataType)
    {
        return new VMComplexCell(dataType);
    }

    @Override
    public List<VMCell> getCells()
    {
        return cells;
    }

    // add new cell to the existing list of cells
    @Override
    public void addCell(MSimpleCell cell)
    {
        cells.add((VMCell) cell);
    }
}
