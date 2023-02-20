package com.guagua;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author guagua
 * @date 2023/2/20 18:20
 * @describe
 */
public class HbaseDML {

    private static Connection connection = HbaseConnection.getConnection();

    /**
     * 添加数据
     *
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     */
    public static void putCell(String namespace, String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {

        if (!HbaseDDL.isTableExist(namespace, tableName)) {
            throw new RuntimeException("表不存在");
        }
        TableName name = TableName.valueOf(namespace, tableName);
        Table table = connection.getTable(name);

        try {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes());

            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    /**
     * 读取单元格数据
     *
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @return
     * @throws IOException
     */
    public static Cell[] getCells(String namespace, String tableName, String rowKey, String columnFamily, String column) throws IOException {
        HbaseDDL.validate(namespace, tableName);
        if (!HbaseDDL.isTableExist(namespace, tableName)) {
            return null;
        }
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Get get = new Get(rowKey.getBytes());
        get.addColumn(columnFamily.getBytes(), column.getBytes());

        get.readAllVersions();

        Result result = table.get(get);

        Cell[] cells = result.rawCells();

//        printCells(cells);

        table.close();

        return cells;
    }

    private static void printCells(Cell[] cells) {
        for (Cell cell : cells) {
            String value = new String(CellUtil.cloneValue(cell));
            System.out.println(value);
        }
    }


    public static void main(String[] args) throws IOException {
        String namespace = "bigdata";
        String table = "user";
        String rowKey = "1001";
        String columFamily = "info";
        String column = "name";

//        putCell(namespace, table, "1001", "info", "name", "guagua");
//        putCell(namespace, table, "1001", "info", "name", "dudu");
//        putCell(namespace, table, "1001", "info", "name", "kehui");

        Cell[] cells = getCells(namespace, table, rowKey, columFamily, column);

        printCells(cells);

    }
}
