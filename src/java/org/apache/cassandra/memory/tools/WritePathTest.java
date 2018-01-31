package org.apache.cassandra.memory.tools;

public class WritePathTest {
/*
    public void dumpPMStorageData() {
        for (Entry<PersistentString, PMTablesManager> ksMgr : PMStorageWrapper.ksManager.entrySet()) {
            System.out.println("keyspace: " + ksMgr.getKey());
            dumpPMTablesManagerData(ksMgr.getValue());
        }
    }

    private void dumpPMTablesManagerData(PMTablesManager pmTablesManager) {
        for (Entry<PersistentUUID, PMTable> pmTablesMgr : pmTablesManager.getPMTablesManager().entrySet()) {
            System.out.println("UUID: " + pmTablesMgr.getKey() + " Name: " + pmTablesMgr.getValue().getTableMetadata
                    ().getTableName());
            dumpPMTableData(pmTablesMgr.getValue());
        }
    }

    public void displayPMStorageStats() {
        for (Entry<PersistentString, PMTablesManager> ksMgr : PMStorageWrapper.ksManager.entrySet()) {
            System.out.println("keyspace: " + ksMgr.getKey());
            for (Entry<PersistentUUID, PMTable> uuidpmTableEntry : ksMgr.getValue().getPMTablesManager().entrySet()) {
                boolean isCKAvailable = uuidpmTableEntry.getValue
                        ().doesClusteringKeyExist();
                System.out.println("table name: " + uuidpmTableEntry.getValue().getTableMetadata().getTableName() + "" +
                        " Partitions# " +
                        uuidpmTableEntry.getValue().getPMTable().size() + " Clustering Key: " + isCKAvailable);
                if (isCKAvailable) {
                    int pos = 0;
                    for (Entry<PersistentDecoratedKey, PersistentObject> keyObjectEntry : uuidpmTableEntry.getValue()
                            .getPMTable()
                            .entrySet()) {
                        System.out.println("Partition# " + (++pos) + " Rows# " + ((PersistentColumnFamilySortedMap)
                                keyObjectEntry.getValue()).getSortedMap().size());
                    }
                }
            }
        }
    }

    private void dumpPMTableData(PMTable pmTable) {
        try {
            for (Entry<PersistentDecoratedKey, PersistentObject> pmTableEntry : pmTable.getPMTable()
                    .entrySet()) {
                System.out.println("PartitionKey: " + pmTableEntry.getKey().getKey().toString());
                if (pmTable.doesClusteringKeyExist())
                    System.out.println("Clustering key exists");
                else
                    System.out.println("Clustering key DOES NOT exist");
                if (pmTable.tableMetadata.name.equalsIgnoreCase("compaction_history")) {
                    // We don't expect clustering key in compaction_history
                    System.out.println("[");
                    dumpCompactionHistory(((PersistentRowSingle) pmTableEntry.getValue()).getRow());
                    System.out.println("]");
                } else if (pmTable.tableMetadata.name.equalsIgnoreCase("local")) {
                    //dumpLocalPMTable((PersistentRow) pmTableEntry.getValue());
                    dumpLocalPMTable(((PersistentRowSingle) pmTableEntry.getValue()).getRow());
                } else if (pmTable.tableMetadata.name.equalsIgnoreCase("size_estimates")) {
//                    dumpLocalPMTable((PersistentRow) pmTableEntry.getValue());
                } else if (pmTable.tableMetadata.name.equalsIgnoreCase("columns")) {
                    System.out.println("Key : " + ByteBufferUtil.string(ByteBuffer.wrap(pmTableEntry.getKey().getKey
                                    ().array()),
                            StandardCharsets.UTF_8));

                    if (pmTable.doesClusteringKeyExist())
                        dumpColumnsPMTable((PersistentColumnFamilySortedMap) pmTableEntry.getValue());
                } else if (pmTable.tableMetadata.name.equalsIgnoreCase("tables")) {
                    String keyValue = ByteBufferUtil.string(ByteBuffer.wrap(pmTableEntry.getKey().getKey().array()),
                            StandardCharsets.UTF_8);
                    System.out.println("Key : " + keyValue);

                    if (pmTable.doesClusteringKeyExist())
                        dumpTablesPMTable((PersistentColumnFamilySortedMap) pmTableEntry.getValue());
                } else if (pmTable.tableMetadata.name.equalsIgnoreCase("keyspaces")) {
                    String keyValue = ByteBufferUtil.string(ByteBuffer.wrap(pmTableEntry.getKey().getKey().array()),
                            StandardCharsets.UTF_8);
                    System.out.println("Key : " + keyValue);

                    //dumpKeyspacesPMTable((PersistentRow) pmTableEntry.getValue());
                    dumpKeyspacesPMTable(((PersistentRowSingle) pmTableEntry.getValue()).getRow());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dumpKeyspacesPMTable(PersistentRow row) {
        try {
            System.out.println("row liveness_info { tstamp: " + dateString(TimeUnit.MICROSECONDS, row
                    .getPrimaryKeyLivenessTimestamp(), false) + " }");
            for (Entry<PersistentString, PersistentCellBase> cellEntry : row.getCells().entrySet()) {
                System.out.print(cellEntry.getKey() + " : ");
                if (cellEntry.getValue().getType() == PersistentCellType.getCellType(MConfig.CELL_TYPE_SIMPLE)) {
                    PersistentCell cell = (PersistentCell) cellEntry.getValue();
                    if (cell.getValue() != null) {
                        System.out.print(toDisplayString(cell.getDataType(), ByteBuffer.wrap(cell.getValue().toArray
                                ())) + " ");
                    }
                } else if (cellEntry.getValue().getType() == PersistentCellType.getCellType(MConfig
                        .CELL_TYPE_COMPLEX)) {
                    PersistentComplexCell cell = (PersistentComplexCell) cellEntry.getValue();
                    PersistentArrayList<PersistentCell> complexCells = cell.getCells();
                    System.out.println();
                    for (int i = 0; i < complexCells.size(); i++) {
                        if (complexCells.get(i).getCellPathDataType() != null)
                            System.out.print("path: [ " + toDisplayString(complexCells.get(i).getCellPathDataType
                                    (), ByteBuffer.wrap(complexCells.get(i).getCellPath().toArray())) + " ] ");
                        else
                            System.out.print("path: ");
                        if (complexCells.get(i).getValue() != null)
                            System.out.print("value: " + toDisplayString(complexCells.get(i).getDataType(),
                                    ByteBuffer.wrap(complexCells.get(i).getValue().toArray())) + " }");
                        else
                            System.out.print(" value: ");
                        System.out.println("tstamp: " + dateString(TimeUnit.MICROSECONDS, complexCells.get(i)
                                .getTimestamp(), false));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dumpTablesPMTable(PersistentColumnFamilySortedMap sortedMap) {
        try {

            for (Entry<PersistentClusteringKey, PersistentRow> rowEntry : sortedMap.getSortedMap().entrySet()) {
                System.out.print("Clustering: [ ");
                PersistentClusteringKey clusteringKey = rowEntry.getKey();
                for (int i = 0; i < clusteringKey.getColNames().size(); i++) {
                    System.out.print(clusteringKey.getColName(i) + " : ");
                    System.out.print(toDisplayString(clusteringKey.getDataType(i), ByteBuffer.wrap(clusteringKey
                            .getValue(i).array()))
                            + ", ");
                }
                System.out.println("]");

                System.out.println("row liveness_info { tstamp: " + dateString(TimeUnit.MICROSECONDS, rowEntry
                        .getValue()
                        .getPrimaryKeyLivenessTimestamp(), false) + " }");
                for (Entry<PersistentString, PersistentCellBase> cellEntry : rowEntry.getValue().getCells().entrySet
                        ()) {
                    System.out.print(cellEntry.getKey() + " : ");
                    if (cellEntry.getValue().getType() == PersistentCellType.getCellType(MConfig.CELL_TYPE_SIMPLE)) {
                        PersistentCell cell = (PersistentCell) cellEntry.getValue();
                        if (cell.getValue() != null) {
                            System.out.println(toDisplayString(cell.getDataType(), ByteBuffer.wrap(cell.getValue()
                                    .toArray()))
                                    + " ");
                        }
                    } else if (cellEntry.getValue().getType() == PersistentCellType.getCellType(MConfig
                            .CELL_TYPE_COMPLEX)) {
                        System.out.println("Not expected.");
                    }
                }


            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dumpColumnsPMTable(PersistentColumnFamilySortedMap sortedMap) {
        try {

            for (Entry<PersistentClusteringKey, PersistentRow> rowEntry : sortedMap.getSortedMap().entrySet()) {
                System.out.print("Clustering: [ ");
                PersistentClusteringKey clusteringKey = rowEntry.getKey();
                for (int i = 0; i < clusteringKey.getColNames().size(); i++) {
                    System.out.print(clusteringKey.getColNames().get(i) + " : ");
                    System.out.print(toDisplayString(clusteringKey.getDataType(i), ByteBuffer.wrap(clusteringKey
                            .getValue(i).array()))
                            + ", ");
                }
                System.out.println("]");
                System.out.println("row liveness_info { tstamp: " + dateString(TimeUnit.MICROSECONDS, rowEntry
                        .getValue()
                        .getPrimaryKeyLivenessTimestamp(), false) + " }");
                for (Entry<PersistentString, PersistentCellBase> cellEntry : rowEntry.getValue().getCells().entrySet
                        ()) {
                    System.out.print(cellEntry.getKey() + " : ");
                    if (cellEntry.getValue().getType() == PersistentCellType.getCellType(MConfig.CELL_TYPE_SIMPLE)) {
                        PersistentCell cell = (PersistentCell) cellEntry.getValue();
                        if (cell.getValue() != null) {
                            System.out.println(toDisplayString(cell.getDataType(), ByteBuffer.wrap(cell.getValue()
                                    .toArray()))
                                    + " ");
                        }
                    } else if (cellEntry.getValue().getType() == PersistentCellType.getCellType(MConfig
                            .CELL_TYPE_COMPLEX)) {
                        System.out.println("Not expected.");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO: fix row timestamp and schema_version
    private void dumpLocalPMTable(PersistentRow row) {
        try {
            System.out.println("row liveness_info { tstamp: " + dateString(TimeUnit.MICROSECONDS, row
                    .getPrimaryKeyLivenessTimestamp(), false) + " }");
            for (Entry<PersistentString, PersistentCellBase> cellEntry : row.getCells().entrySet()) {
                System.out.print(cellEntry.getKey() + " : ");
                if (cellEntry.getValue().getType() == PersistentCellType.getCellType(MConfig.CELL_TYPE_SIMPLE)) {
                    PersistentCell cell = (PersistentCell) cellEntry.getValue();
                    if (cell.getValue() != null) {
                        System.out.print(toDisplayString(cell.getDataType(), ByteBuffer.wrap(cell.getValue().toArray
                                ())) + " ");
                        System.out.println("tstamp: " + dateString(TimeUnit.MICROSECONDS, cell.getTimestamp(), false));
                    }
                } else if (cellEntry.getValue().getType() == PersistentCellType.getCellType(MConfig
                        .CELL_TYPE_COMPLEX)) {
                    PersistentComplexCell cell = (PersistentComplexCell) cellEntry.getValue();
                    PersistentArrayList<PersistentCell> complexCells = cell.getCells();
                    System.out.println();
                    for (int i = 0; i < complexCells.size(); i++) {
                        if (complexCells.get(i).getCellPathDataType() != null)
                            System.out.print("path: [ " + toDisplayString(complexCells.get(i).getCellPathDataType
                                    (), ByteBuffer.wrap(complexCells.get(i).getCellPath().toArray())) + " ] ");
                        else
                            System.out.print("path: ");
                        if (complexCells.get(i).getValue() != null)
                            System.out.print("value: " + toDisplayString(complexCells.get(i).getDataType(),
                                    ByteBuffer.wrap(complexCells.get(i).getValue().toArray())) + " }");
                        else
                            System.out.print(" value: ");
                        System.out.println("tstamp: " + dateString(TimeUnit.MICROSECONDS, complexCells.get(i)
                                .getTimestamp(), false));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO: implementation is not complete
    private void dumpCompactionHistory(PersistentRow row) {
        System.out.print(" row liveness_info { tstamp: " + dateString(TimeUnit.MICROSECONDS, row
                .getPrimaryKeyLivenessTimestamp(), false));
        System.out.print(" ttl: " + row.getPrimaryKeyTTL());
        System.out.print(" expires_at: " + dateString(TimeUnit.SECONDS, row.getPkLocalExpirationTime(),
                false));
        System.out.println(" expired: " + (row.getPkLocalExpirationTime() < (System.currentTimeMillis() / 1000))
                + "}");

        System.out.println("{");
        for (Entry<PersistentString, PersistentCellBase> columnData : row.getCells().entrySet()) {
            System.out.print(columnData.getKey() + ": ");
            if (columnData.getValue().getType() == PersistentCellType.getCellType(MConfig.CELL_TYPE_SIMPLE)) {
                PersistentCell cell = (PersistentCell) columnData.getValue();
                System.out.println(toDisplayString(cell.getDataType(), ByteBuffer.wrap(cell.getValue().toArray())));
            } else if (columnData.getValue().getType() == PersistentCellType.getCellType(MConfig.CELL_TYPE_COMPLEX)) {
                PersistentComplexCell cell = (PersistentComplexCell) columnData.getValue();
                System.out.print("deletion_info { marked_deleted: " + dateString(TimeUnit.MICROSECONDS, cell
                        .getComplexDeletionTime().markedForDeleteAt(), false) + " ,local_delete_time: " +
                        dateString(TimeUnit.SECONDS, cell.getComplexDeletionTime().localDeletionTime(),
                                false));
                PersistentArrayList<PersistentCell> complexCells = cell.getCells();
                for (int i = 0; i < complexCells.size(); i++) {
                    if (complexCells.get(i).getCellPathDataType() != null)
                        System.out.print("path: [ " + toDisplayString(complexCells.get(i).getCellPathDataType
                                (), ByteBuffer.wrap(complexCells.get(i).getCellPath().toArray())) + " ] ");
                    System.out.println("value: " + toDisplayString(complexCells.get(i).getDataType(),
                            ByteBuffer.wrap(complexCells.get(i).getValue().toArray())) + " }");
                }
            } else {
                System.out.println("Error: Invalid cell type");
            }
        }
        System.out.println("}");
    }

    // Helper methods
    private String dateString(TimeUnit from, long time, boolean rawTime) {
        if (rawTime)
            return Long.toString(time);

        long secs = from.toSeconds(time);
        long offset = Math.floorMod(from.toNanos(time), 1000_000_000L);
        return Instant.ofEpochSecond(secs, offset).toString();
    }

    private String getUuidFromByteArray(byte[] bytes) {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            buffer.append(String.format("%02x", bytes[i]));
        }
        return buffer.toString();
    }

    private String toDisplayString(PMDataTypes dataType, ByteBuffer bb) {
        try {
            if (dataType == PMDataTypes.getDataType(MConfig.PM_INTEGER_TYPE))
                return ByteBufferUtil.toInt(bb) + "";
            else if (dataType == PMDataTypes.getDataType(MConfig.PM_LONG_TYPE))
                return ByteBufferUtil.toLong(bb) + "";
            else if (dataType == PMDataTypes.getDataType(MConfig.PM_UTF8_TYPE) || dataType == PMDataTypes
                    .getDataType(MConfig.PM_STRING_TYPE))
                return ByteBufferUtil.string(bb, StandardCharsets.UTF_8);
            else if (dataType == PMDataTypes.getDataType(MConfig.PM_TIMESTAMP_TYPE))
                return new Date(ByteBufferUtil.toLong(bb)).toString();
            else if (dataType == PMDataTypes.getDataType(MConfig.PM_UUID_TYPE)) {
                String abc;
                byte[] temp = new byte[bb.remaining()];
                bb.get(temp);
                abc = getUuidFromByteArray(temp).toString();
                return abc;
            } else if (dataType == PMDataTypes.getDataType(MConfig.PM_INET_ADDRESS_TYPE))
                return InetAddress.getByAddress(bb.array()) + "";
            else if (dataType == PMDataTypes.getDataType(MConfig.PM_BLOB_TYPE))
                return ByteBufferUtil.bytesToHex(bb) + "";
            else if (dataType == PMDataTypes.getDataType(MConfig.PM_DOUBLE_TYPE))
                return ByteBufferUtil.toDouble(bb) + "";
            else if (dataType == PMDataTypes.getDataType(MConfig.PM_BOOLEAN_TYPE))
                return (bb.get(bb.position()) != 0) ? "true" : "false";
            else
                return "";

        } catch (Exception e) {
            e.printStackTrace();
            ;
            return "";
        }

    }*/
}
