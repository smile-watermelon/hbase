package com.guagua;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author guagua
 * @date 2023/2/20 15:01
 * @describe
 */
public class HbaseDDL {

    public static Connection connection = HbaseConnection.getConnection();

    public static void createNamespace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        try {

            NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
            builder.addConfiguration("creator", "guagua");

            NamespaceDescriptor descriptor = builder.build();
            admin.createNamespace(descriptor);
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void deleteNamespace(String name) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            admin.deleteNamespace(name);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void disableTable(String namespace, String tableName) throws IOException {
        validate(namespace, tableName);
        Admin admin = connection.getAdmin();
        try {
            TableName name = TableName.valueOf(namespace, tableName);
            admin.disableTable(name);
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
    }

    public static void createTable(String namespace, String tableName, String... families) throws IOException {
        validate(namespace, tableName);
        if (families == null || families.length == 0) {
            throw new RuntimeException("???????????????families ??????");
        }
        Admin admin = connection.getAdmin();

        try {
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));

            for (String family : families) {

                ColumnFamilyDescriptorBuilder columnBuilder = ColumnFamilyDescriptorBuilder
                        .newBuilder(family.getBytes(StandardCharsets.UTF_8));

                columnBuilder.setMaxVersions(5);

                builder.setColumnFamily(columnBuilder.build());
            }
            TableDescriptor descriptor = builder.build();

            admin.createTable(descriptor);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }



    public static void deleteTable(String namespace, String tableName) throws IOException {
        validate(namespace, tableName);
        if (!isTableExist(namespace, tableName)) {
            return;
        }
        Admin admin = connection.getAdmin();

        try {
            TableName name = TableName.valueOf(namespace, tableName);
            admin.disableTable(name);
            admin.deleteTable(name);
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();
    }

    public static void modifyTable(String namespace, String tableName, String family) throws IOException, RuntimeException {
        validate(namespace, tableName);
        if (!isTableExist(namespace, tableName)) {
            throw new RuntimeException("?????????????????????");
        }

        Admin admin = connection.getAdmin();

        // ???????????????????????????
        TableDescriptor td = admin.getDescriptor(TableName.valueOf(namespace, tableName));

        // ???????????????????????????????????????????????????
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(td);

        // ????????????????????????
        ColumnFamilyDescriptor columnFamilyDescriptor = td.getColumnFamily(family.getBytes());

        // ??????????????????????????????????????????
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyDescriptor);

        columnFamilyDescriptorBuilder.setMaxVersions(4);

        // ????????????
        tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());

        try {
            // ?????????
            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
    }

    public static boolean isTableExist(String namespace, String tableName) throws IOException {
        validate(namespace, tableName);
        Admin admin = connection.getAdmin();
        boolean result = false;
        try {
            TableName name = TableName.valueOf(namespace, tableName);
            result = admin.tableExists(name);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }


    public static void validate(String namespace, String tableName) {
        if (!StringUtils.isNoneBlank(namespace)) {
            throw new RuntimeException("?????????????????????");
        }
        if (!StringUtils.isNoneBlank(tableName)) {
            throw new RuntimeException("???????????????");
        }
    }



    public static void main(String[] args) throws IOException {
        String namespace = "bigdata";
        String table = "user1";
//        createNamespace("bigdata");
//        deleteNamespace(table);
//        deleteTable(table);

//        boolean b = isTableExist(namespace, table);
//        if (b) {
//            disableTable(namespace, "");
//            deleteTable(namespace, table);
//            createTable(namespace, table, "info", "address");
//        }

        modifyTable(namespace, table, "info");

        System.out.println();
        HbaseConnection.close();
    }
}
