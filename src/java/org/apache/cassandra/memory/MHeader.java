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

import java.util.List;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;

public class MHeader
{
    public final AbstractType<?> keyType;
    public final List<AbstractType<?>> clusteringTypes;

    public final RegularAndStaticColumns columns;

    public MHeader(AbstractType<?> keyType,
                   List<AbstractType<?>> clusteringTypes,
                   RegularAndStaticColumns columns)
    {
        this.keyType = keyType;
        this.clusteringTypes = clusteringTypes;
        this.columns = columns;
    }

    public Columns columns(boolean isStatic)
    {
        return isStatic ? columns.statics : columns.regulars;
    }

    public List<AbstractType<?>> clusteringTypes()
    {
        return clusteringTypes;
    }

    public AbstractType<?> getType(ColumnMetadata column)
    {
        return column.type;
    }
}
