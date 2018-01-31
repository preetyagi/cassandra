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
import lib.util.persistent.PersistentArrayList;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.memory.MCellType;
import org.apache.cassandra.memory.MComplexCell;
import org.apache.cassandra.memory.MConfig;
import org.apache.cassandra.memory.MDataTypes;
import org.apache.cassandra.memory.MDeletionTime;
import org.apache.cassandra.memory.MSimpleCell;

public final class PersistentComplexCell extends PersistentCellBase implements MComplexCell
{
    private static final ObjectField<PersistentArrayList<PersistentCell>> CELLS = new ObjectField<>();
    private static final ObjectField<PersistentDeletionTime> COMPLEX_DELETION_TIME = new ObjectField<>();
    private static final ObjectField<PMDataTypes> COMPLEX_DATA_TYPE = new ObjectField<>();

    private static final ObjectType<PersistentComplexCell> TYPE = PersistentCellBase.TYPE.extendWith
                                                                                          (PersistentComplexCell.class,
                                                                                           CELLS,
                                                                                           COMPLEX_DELETION_TIME,
                                                                                           COMPLEX_DATA_TYPE);

    // constructor
    public PersistentComplexCell(MDataTypes dataType, boolean setComplexDeletion, MDeletionTime deletionTime)
    {
        super(TYPE, MCellType.getType(MConfig.CELL_TYPE_COMPLEX), (PersistentComplexCell self) ->
        {
            self.initObjectField(CELLS, new PersistentArrayList<>());
            self.initObjectField(COMPLEX_DATA_TYPE, (PMDataTypes) dataType);
            if (setComplexDeletion)
                self.initObjectField(COMPLEX_DELETION_TIME, (PersistentDeletionTime) deletionTime);
        });
    }

    // reconstructor
    private PersistentComplexCell(ObjectPointer<? extends PersistentComplexCell> pointer)
    {
        super(pointer);
    }

    @Override
    public PersistentArrayList<PersistentCell> getCells()
    {
        return getObjectField(CELLS);
    }

    // add new cell to the existing list of cells
    @Override
    public void addCell(MSimpleCell cell)
    {
        getCells().add((PersistentCell) cell);
    }
}
