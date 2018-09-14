package com.flipkart.yak.sep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static com.flipkart.yak.sep.BaseTest.cf1;
import static com.flipkart.yak.sep.BaseTest.tableName;

/*
 * Created by Amanraj on 09/08/18 .
 */

public class InvalidSepConfWithoutDefaultTestCase {

    private static Configuration conf = HBaseConfiguration.create();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        conf.set("sep.kafka.config.path", "src/test/resources/sep-conf-invalid.json");
        BaseTest.setup(conf);
    }

    @Test
    public void invalidConfigTest() throws Exception {

        byte[] column = "{Message}".getBytes();
        byte[] rowKey = "row1".getBytes();
        byte[] qualifier = "data".getBytes();

        doPut(cf1, rowKey, qualifier, column);

        try {
            KafkaConsumer.readMessages("yak_export", 1);
            Assert.assertFalse(true);

        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }


    private void doPut(byte[] columnFamily, byte[] rowKey, byte[] qualifier, byte[] column) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        try (Table t = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(rowKey);
            put.addColumn(columnFamily, qualifier, column);
            t.put(put);
        }
    }


    @AfterClass
    public static void afterClass() throws Exception {
        BaseTest.close();
    }
}
