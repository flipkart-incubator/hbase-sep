package com.flipkart.yak.sep;

/*
 * Created by Amanraj on 08/08/18 .
 */

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
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto;

import java.io.IOException;
import java.util.List;

import static com.flipkart.yak.sep.BaseTest.*;


public class KafkaReplicationEndPointTestCase {

    private static Configuration conf = HBaseConfiguration.create();


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        conf.set("sep.kafka.config.path", "src/test/resources/sep-conf.json");
        BaseTest.setup(conf);
    }


    @Test
    public void nonWhiteListedQualifierTest() throws Exception {

        byte[] column = "{Message}".getBytes();
        byte[] rowKey = "row1".getBytes();
        byte[] qualifier = "value".getBytes();

        doPut(cf1, rowKey, qualifier, column);

        try {
            KafkaConsumer.readMessages("yak_export", 1);
            Assert.assertFalse(true);

        } catch (Exception e) {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void whiteListedQualifierTest() throws Exception {


        byte[] column = "{Message}".getBytes();
        byte[] rowKey = "row1".getBytes();
        byte[] qualifier = "data".getBytes();

        doPut(cf1, rowKey, qualifier, column);
        doPut(cf2, rowKey, qualifier, column);

        try {
            List<byte[]> messages = KafkaConsumer.readMessages("yak_export", 2);

            for (byte[] message : messages) {
                SepMessageProto.SepMessage sepMessage = SepMessageProto.SepMessage.parseFrom(message);

                byte[] cfBytes = new byte[sepMessage.getColumnfamily().size()];
                sepMessage.getColumnfamily().copyTo(cfBytes, 0);

                byte[] valueBytes = new byte[sepMessage.getValue().size()];
                sepMessage.getValue().copyTo(valueBytes, 0);

                Assert.assertTrue(new String(cfBytes).equals(new String(cf1)) || new String(cfBytes).equals(new String(cf2)));
                Assert.assertEquals(new String(valueBytes), new String(column));
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertFalse(true);
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
