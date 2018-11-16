package com.flipkart.yak.sep;

import info.batey.kafka.unit.KafkaUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;

/*
 * Created by Amanraj on 09/08/18 .
 */

@SuppressWarnings("WeakerAccess")
public class BaseTest {

    private static HBaseTestingUtility utility;
    private static ReplicationAdmin admin;

    protected static final byte[] cf1 = "cf".getBytes();
    protected static final byte[] cf2 = "cc".getBytes();
    protected static final byte[] tableName = Bytes.toBytes("test");

    private static final int zkPort = 9002;
    private static final int kafkaPort = 9003;

    private static KafkaUnit kafkaUnitServer;

    protected static void setup(Configuration conf) throws Exception {

        utility = new HBaseTestingUtility(conf);
        utility.startMiniZKCluster();

        admin = new ReplicationAdmin(conf);
        utility.startMiniCluster(1);

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor fam1 = new HColumnDescriptor(cf1);
        fam1.setMaxVersions(3);
        fam1.setScope(1);
        table.addFamily(fam1);

        HColumnDescriptor fam2 = new HColumnDescriptor(cf2);
        fam2.setMaxVersions(3);
        fam2.setScope(1);
        table.addFamily(fam2);
        utility.getHBaseAdmin().createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);

        admin.addPeer("testKafkaReplicationEndpoint",
                new ReplicationPeerConfig().setClusterKey(ZKConfig.getZooKeeperClusterKey(utility.getConfiguration()))
                        .setReplicationEndpointImpl(KafkaReplicationEndPoint.class.getName()), null);

        startKafkaServer();
    }

    private static void startKafkaServer() throws Exception {
        kafkaUnitServer = new KafkaUnit(zkPort, kafkaPort);
        kafkaUnitServer.startup();
        kafkaUnitServer.createTopic("yak_export");
    }

    protected static void close() throws Exception {

        if (admin != null) {
            admin.removePeer("testKafkaReplicationEndpoint");
            admin.close();
        }

        if (utility != null) {
            utility.shutdownMiniCluster();
        }

        if (kafkaUnitServer != null) {
            kafkaUnitServer.shutdown();
        }
    }
}
