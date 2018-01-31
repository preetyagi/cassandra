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

import java.nio.ByteBuffer;

import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentArrayList;
import lib.util.persistent.PersistentByteBuffer;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.PersistentString;
import lib.util.persistent.Transaction;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MConfig;
import org.apache.cassandra.memory.MDataTypes;
import org.apache.cassandra.utils.FastByteOperations;

/**
 * An instance of this class represents the clustering key information for a specific row.
 * It includes column names, data types and values. Depending on the data type of each column
 * included in clustering key, the comparision happens to keep rows sorted within a partition.
 */
public final class PersistentClusteringKey extends PersistentImmutableObject implements Comparable<PersistentClusteringKey>, MClusteringKey
{
    private static final ObjectField<PersistentArrayList<PersistentString>> COL_NAMES = new ObjectField<>();
    private static final ObjectField<PersistentArrayList<PMDataTypes>> CLUSTERING_TYPE = new ObjectField<>();
    private static final ObjectField<PersistentArrayList<PersistentByteBuffer>> VALUE = new ObjectField<>();

    private static final ObjectType<PersistentClusteringKey> TYPE = ObjectType.fromFields(PersistentClusteringKey
                                                                                          .class,
                                                                                          COL_NAMES,
                                                                                          CLUSTERING_TYPE,
                                                                                          VALUE);

    private int numColNames = 0;

    // constructor
    public PersistentClusteringKey()
    {
        super(TYPE, (PersistentImmutableObject self) ->
        {
            self.initObjectField(COL_NAMES, new PersistentArrayList<>());
            self.initObjectField(CLUSTERING_TYPE, new PersistentArrayList<>());
            self.initObjectField(VALUE, new PersistentArrayList<>());
        });
    }

    // reconstructor
    private PersistentClusteringKey(ObjectPointer<? extends PersistentClusteringKey> pointer)
    {
        super(pointer);
        numColNames = getColNames().size();
    }

    private void addColName(PersistentString colName)
    {
        Transaction.run(() ->
                        {
                            getColNames().add(colName);
                        }, () ->
                        {
                            numColNames++;
                        });
    }

    private PersistentArrayList<PersistentString> getColNames()
    {
        return getObjectField(COL_NAMES);
    }

    private void addDataType(MDataTypes dataType)
    {
        getDataTypes().add((PMDataTypes) dataType);
    }

    public MDataTypes getDataType(int pos)
    {
        return getDataTypes().get(pos);
    }

    private PersistentArrayList<PMDataTypes> getDataTypes()
    {
        return getObjectField(CLUSTERING_TYPE);
    }

    private void addValue(PersistentByteBuffer value)
    {
        getValues().add(value);
    }

    private PersistentByteBuffer getValue(int pos)
    {
        return getValues().get(pos);
    }

    private PersistentArrayList<PersistentByteBuffer> getValues()
    {
        return getObjectField(VALUE);
    }

    @Override
    public byte[] getValueBuf(int pos)
    {
        return getValues().get(pos).array();
    }

    @Override
    public int getNumKeys()
    {
        //return getColNames().size();
        return numColNames;
    }

    @Override
    public void addClusteringKeyColumn(String colName, MDataTypes dataType, byte[] value)
    {
        if (colName.length() == 0 || dataType == MDataTypes.getType(MConfig.PM_INVALID_TYPE))
        {
            System.out.println("Nothing to add in clustering key: " + colName.length());
            return;
        }
        addColName(PersistentString.make(colName));
        addDataType(dataType);
        addValue(PersistentByteBuffer.copyWrap(value));
    }

    @Override
    public int compareTo(PersistentClusteringKey o)
    {
        // compare lengths first. Ideally it should never happen!!!
        if (getNumKeys() < o.getNumKeys())
        {
            return -1;
        }
        else if (getNumKeys() > o.getNumKeys())
        {
            return 1;
        }

        int length = getNumKeys();
        int cmp = -1;
        for (int i = 0; i < length; i++)
        {
            PMDataTypes dataType = getObjectField(CLUSTERING_TYPE).get(i);
            if (dataType == MDataTypes.getType(MConfig.PM_BOOLEAN_TYPE))
            {
                ByteBuffer b1 = ByteBuffer.wrap(getValue(i).array());
                ByteBuffer b2 = ByteBuffer.wrap(o.getValue(i).array());
                cmp = BooleanType.instance.compareCustom(b1, b2);
            }
            else if (dataType == MDataTypes.getType(MConfig.PM_INTEGER_TYPE))
            {
                ByteBuffer b1 = ByteBuffer.wrap(getValue(i).array());
                ByteBuffer b2 = ByteBuffer.wrap(o.getValue(i).array());
                cmp = IntegerType.instance.compareCustom(b1, b2);
            }
            else if (dataType == MDataTypes.getType(MConfig.PM_DOUBLE_TYPE))
            {
                ByteBuffer b1 = ByteBuffer.wrap(getValue(i).array());
                ByteBuffer b2 = ByteBuffer.wrap(o.getValue(i).array());
                cmp = DoubleType.instance.compareCustom(b1, b2);
            }
            else if (dataType == MDataTypes.getType(MConfig.PM_FLOAT_TYPE))
            {
                ByteBuffer b1 = ByteBuffer.wrap(getValue(i).array());
                ByteBuffer b2 = ByteBuffer.wrap(o.getValue(i).array());
                cmp = FloatType.instance.compareCustom(b1, b2);
            }
            else if (dataType == MDataTypes.getType(MConfig.PM_TIMEUUID_TYPE))
            {
                ByteBuffer b1 = ByteBuffer.wrap(getValue(i).array());
                ByteBuffer b2 = ByteBuffer.wrap(o.getValue(i).array());
                cmp = TimeUUIDType.instance.compareCustom(b1, b2);
            }
            else if (dataType == MDataTypes.getType(MConfig.PM_LONG_TYPE))
            {
                ByteBuffer b1 = ByteBuffer.wrap(getValue(i).array());
                ByteBuffer b2 = ByteBuffer.wrap(o.getValue(i).array());
                cmp = LongType.instance.compareCustom(b1, b2);
            }
            else
            {
                byte[] b1 = getValue(i).array();
                byte[] b2 = o.getValue(i).array();
                cmp = FastByteOperations.compareUnsigned(b1, 0, b1.length, b2, 0, b2.length);
            }

            if (cmp != 0)
                break;
        }
        return cmp;
    }

    public int hashCode()
    {
        return (getValue(0).hashCode());
    }

    public boolean equals(Object obj)
    {
        // sanity checks
        if (obj == null)
            return false;
        else if (this == obj)
            return true;
        else if (getClass() != obj.getClass())
            return false;

        return compareTo((PersistentClusteringKey) obj) == 0 ? true : false;
    }
}
