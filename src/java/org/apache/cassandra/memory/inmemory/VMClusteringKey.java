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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MConfig;
import org.apache.cassandra.memory.MDataTypes;
import org.apache.cassandra.utils.FastByteOperations;

/**
 * An instance of this class represents the clustering key information for a specific row.
 * It includes column names, data types and values. Depending on the data type of each column
 * included in clustering key, the comparision happens to keep rows sorted within a partition.
 */
public final class VMClusteringKey implements Comparable<VMClusteringKey>, MClusteringKey
{
    private List<String> colNames;
    private List<MDataTypes> clusteringType;
    private List<ByteBuffer> value;
    private int numKeys;

    // constructor
    private VMClusteringKey()
    {
        colNames = new ArrayList<>();
        clusteringType = new ArrayList<>();
        value = new ArrayList<>();
        numKeys = 0;
    }

    public static VMClusteringKey getInstance()
    {
        return new VMClusteringKey();
    }

    private void addColName(String colName)
    {
        getColNames().add(colName);
    }

    private List<String> getColNames()
    {
        return colNames;
    }

    private void addDataType(MDataTypes dataType)
    {
        this.getDataTypes().add(dataType);
    }

    public MDataTypes getDataType(int pos)
    {
        return this.getDataTypes().get(pos);
    }

    private List<MDataTypes> getDataTypes()
    {
        return clusteringType;
    }

    private void addValue(ByteBuffer value)
    {
        getValues().add(value);
    }

    private ByteBuffer getValue(int pos)
    {
        return value.get(pos);
    }

    private List<ByteBuffer> getValues()
    {
        return value;
    }

    @Override
    public byte[] getValueBuf(int pos)
    {
        return getValues().get(pos).array();
    }

    @Override
    public int getNumKeys()
    {
        return numKeys;
    }

    private void setNumKeys(int val)
    {
        numKeys = val;
    }

    @Override
    public void addClusteringKeyColumn(String colName, MDataTypes dataType, byte[] value)
    {
        if (colName.length() == 0 || dataType == MDataTypes.getType(MConfig.PM_INVALID_TYPE))
        {
            System.out.println("Nothing to add in clustering key: " + colName.length());
            return;
        }
        this.addColName(colName);
        this.addDataType(dataType);
        this.addValue(ByteBuffer.wrap(value));
        this.setNumKeys(getNumKeys() + 1);
    }

    @Override
    public int compareTo(VMClusteringKey o)
    {
        // compare lengths first. Ideally it should never happen!!!
        if (this.getNumKeys() < o.getNumKeys())
        {
            return -1;
        }
        else if (this.getNumKeys() > o.getNumKeys())
        {
            return 1;
        }

        int length = getNumKeys();
        int cmp = 0;
        boolean compareFlag = false;
        for (int i = 0; i < length; i++)
        {
            if (clusteringType.get(i) == MDataTypes.getType(MConfig.PM_BOOLEAN_TYPE))
            {
                cmp = BooleanType.instance.compareCustom(getValue(i), o.getValue(i));
                compareFlag = true;
            }
            else if (clusteringType.get(i) == MDataTypes.getType(MConfig.PM_TIMESTAMP_TYPE))
            {
                cmp = TimestampType.instance.compareCustom(getValue(i), o.getValue(i));
                compareFlag = true;
            }
            else if (clusteringType.get(i) == MDataTypes.getType(MConfig.PM_INTEGER_TYPE))
            {
                cmp = IntegerType.instance.compareCustom(getValue(i), o.getValue(i));
                compareFlag = true;
            }
            else if (clusteringType.get(i) == MDataTypes.getType(MConfig.PM_DOUBLE_TYPE))
            {
                cmp = DoubleType.instance.compareCustom(getValue(i), o.getValue(i));
                compareFlag = true;
            }
            else if (clusteringType.get(i) == MDataTypes.getType(MConfig.PM_FLOAT_TYPE))
            {
                cmp = FloatType.instance.compareCustom(getValue(i), o.getValue(i));
                compareFlag = true;
            }
            else if (clusteringType.get(i) == MDataTypes.getType(MConfig.PM_TIMEUUID_TYPE))
            {
                cmp = TimeUUIDType.instance.compareCustom(getValue(i), o.getValue(i));
                compareFlag = true;
            }
            else if (clusteringType.get(i) == MDataTypes.getType(MConfig.PM_LONG_TYPE))
            {
                cmp = LongType.instance.compareCustom(getValue(i), o.getValue(i));
                compareFlag = true;
            }
            else if (clusteringType.get(i) == MDataTypes.getType(MConfig.PM_BLOB_TYPE))
            {
                byte[] b1 = getValue(i).array();
                byte[] b2 = o.getValue(i).array();
                cmp = FastByteOperations.compareUnsigned(b1, 0, b1.length, b2, 0, b2.length);
                compareFlag = true;
            }
            else if (clusteringType.get(i) == MDataTypes.getType(MConfig.PM_STRING_TYPE))
            {
                byte[] b1 = getValue(i).array();
                byte[] b2 = o.getValue(i).array();
                cmp = FastByteOperations.compareUnsigned(b1, 0, b1.length, b2, 0, b2.length);
                compareFlag = true;
            }
            else if (clusteringType.get(i) == MDataTypes.getType(MConfig.PM_UTF8_TYPE))
            {
                byte[] b1 = getValue(i).array();
                byte[] b2 = o.getValue(i).array();
                cmp = FastByteOperations.compareUnsigned(b1, 0, b1.length, b2, 0, b2.length);
                compareFlag = true;
            }
            if (!compareFlag)
            {
                System.out.println("YOU SHOULDN'T HAVE COME HERE!!! SOMETHING WENT WRONG");
                cmp = -1; // TODO: Remove this
            }
            if (cmp != 0)
                break;
            compareFlag = false; // reset flag
        }
        return cmp;
    }

    // TODO
    public int hashCode()
    {
        return (31 * 17 + getValue(0).hashCode());
    }

    public boolean equals(Object obj)
    {
        // sanity checks
        if (obj == null)
            return false;
        else if (this == obj)
            return true;
        else if (this.getClass() != obj.getClass())
            return false;

        return this.compareTo((VMClusteringKey) obj) == 0 ? true : false;
    }
}
