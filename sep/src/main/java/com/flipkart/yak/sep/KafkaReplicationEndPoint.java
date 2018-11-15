package com.flipkart.yak.sep;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto.SepMessage;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto.SepTableName;
import sep.shade.com.google.protobuf.ByteString;


/**
 * Simple Kafka producer based replication endPoint takes columnFamily = key to
 * kafka producer - (Effectively we isolate kafka producer client per colunm
 * family) takes columnQualifer = topic in kafka brokers takes row = keyId in
 * kafka key message (partition key)
 *

 * "sep.kafka.config.path" => "/tmp/sep_conf.json"
 *
 *
 * @author gokulvanan.v
 *
 */
public class KafkaReplicationEndPoint extends BaseReplicationEndpoint {

    private static final Logger logger = LoggerFactory
            .getLogger(KafkaReplicationEndPoint.class);


    private UUID id;
    private Map<String, Producer<byte[], byte[]>> publisherMap;
    private Map<String, CFConfig> cfConfigMap;
    private static final Object                   lock        = new Object();      // lock to enable hot reload of kafka
    // producer by changing file
    private ExecutorService                       fileWatcherThread;



    private int timeout = 1000; //future.getTimeout
    private static final String DEFAULT_STR = "default";
    private static final ObjectMapper mapper = new ObjectMapper();
    private Configuration                         hbaseConfig;

    @Override
    public UUID getPeerUUID() {
        logger.debug("getPeer UUID {}", id);
        return this.id;
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
        try {
            synchronized (lock) {

                logger.debug("replicate called");

                if (this.publisherMap.isEmpty()) {
                    logger.debug("No replication configured, blindly acking");
                    return true;
                }

                List<Entry> entries = replicateContext.getEntries();
                Map<String, List<Future<RecordMetadata>>> batchFutures = new HashMap<>(this.publisherMap.size());

                for (String cf : this.publisherMap.keySet()) {
                    List<Future<RecordMetadata>> records = new LinkedList<Future<RecordMetadata>>();
                    batchFutures.put(cf, records);
                }

                for (Entry entry : entries) {
                    WALKey key = entry.getKey();
                    WALEdit val = entry.getEdit();
                    TableName tableName = key.getTablename();
                    SepTableName sepTable = SepTableName.newBuilder()
                            .setNamespace(ByteString.copyFrom(tableName.getNamespace()))
                            .setQualifier(ByteString.copyFrom(tableName.getName())).build();

                    String topicPrefix = new StringBuilder().append("yak").append("_")
                            .append(tableName.getNamespaceAsString()).append("_")
                            .append(tableName.getQualifierAsString()).toString();
                    for (Cell cell : val.getCells()) {
                        String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                cell.getQualifierLength());

                        String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
                                cell.getFamilyLength());

                        CFConfig cfConf = null;
                        ;
                        if (cfConfigMap.containsKey(columnFamily)) {
                            cfConf = cfConfigMap.get(columnFamily);
                        } else if (cfConfigMap.containsKey(DEFAULT_STR)) {
                            cfConf = cfConfigMap.get(DEFAULT_STR);
                        } else {
                            // ignore if not applicable.
                            logger.debug("no cfConfig found");
                            continue;
                        }

                        String kafkaProducerLabel;
                        if (publisherMap.containsKey(columnFamily)) {
                            kafkaProducerLabel = columnFamily;
                        } else {
                            kafkaProducerLabel = DEFAULT_STR;
                        }

                        if (!cfConf.getWhiteListedQualifier().isEmpty()
                                && !cfConf.getWhiteListedQualifier().contains(qualifier)) {
                            logger.debug("ignoring not whitelisted qualifier");
                            continue; // ignore not whitelisted if whiltelisted qualifier exists
                        }

                        String topic = new StringBuilder().append(topicPrefix).append("_").append(columnFamily)
                                .append("_").append(qualifier).toString().toLowerCase();

                        if (StringUtils.isNotBlank(cfConf.getDefaultTopicName())) {
                            topic = cfConf.getDefaultTopicName();
                            logger.debug("Changing topicName for column family {} to {}", columnFamily, topic);
                        }

                        if (cfConf.getQualifierToTopicNameMap().containsKey(qualifier)) {
                            // override topicName as per config
                            topic = cfConf.getQualifierToTopicNameMap().get(qualifier);
                            logger.debug("Changing topicName for qualifier {} to {}", qualifier, topic);
                        }

                        if (publisherMap.containsKey(kafkaProducerLabel)) {
                            byte[] groupId = new byte[cell.getRowLength()];
                            System.arraycopy(cell.getRowArray(), cell.getRowOffset(), groupId, 0, groupId.length);
                            logger.debug("pushing to cf {} rowKey {}", kafkaProducerLabel, new String(groupId));


                            SepMessage msg = SepMessageProto.SepMessage.newBuilder()
                                    .setRow(ByteString
                                            .copyFrom(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()))
                                    .setTable(sepTable)
                                    .setColumnfamily(ByteString.copyFrom(cell.getFamilyArray(), cell.getFamilyOffset(),
                                            cell.getFamilyLength()))
                                    .setQualifier(ByteString.copyFrom(cell.getQualifierArray(),
                                            cell.getQualifierOffset(), cell.getQualifierLength()))
                                    .setTimestamp(key.getWriteTime()).setValue(ByteString.copyFrom(cell.getValueArray(),
                                            cell.getValueOffset(), cell.getValueLength()))
                                    .build();
                            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, groupId,
                                    msg.toByteArray());
                            // logger.info("groupId {}, msgRow {}", new String(groupId),msg.getRow());
                            // logger.info("publishing to kafka keySr {}, partition {},", new
                            // String(record.key()), Utils.abs(Utils.murmur2(record.key())) % 16);
                            Future<RecordMetadata> future = this.publisherMap.get(kafkaProducerLabel).send(record);
                            batchFutures.get(kafkaProducerLabel).add(future);
                        } else {
                            logger.debug("No kafka broker configured for this kafkaProducer {}", kafkaProducerLabel);
                            continue;
                        }
                    }

                }

                logger.debug("Flushing futures ");
                // flush all
                for (String cf : batchFutures.keySet()) {
                    List<Future<RecordMetadata>> lst = batchFutures.get(cf);
                    for (Future<RecordMetadata> f : lst) {
                        f.get(timeout, TimeUnit.MILLISECONDS); // this will throw exception if failed
                        // resulting in retries of all
                    }
                }
                logger.debug("Flushed futures");
                return true;

            }
        } catch (Throwable e) {
            logger.error("Error in replicate method call ", e);
            return false;
        }
    }

    private void startKafka() throws Exception {
        logger.info("Starting KafkaReplicationEndPoint ");
        this.publisherMap = new ConcurrentHashMap<>();
        this.cfConfigMap = new HashMap<>();
        String confPath = hbaseConfig.get("sep.kafka.config.path");

        if (confPath != null) {
            SepConfig conf = mapper.readValue(new File(confPath), SepConfig.class);
            this.cfConfigMap = conf.getCfConfig();
            updatePublisherMap();
        } else {
            logger.info("No config.path configured for kafka push");
        }

        // added to ensure timeout happens
        String timeoutKey = "sep.kafka.timeout";
        String timeout = hbaseConfig.get(timeoutKey, "1000");
        logger.info("got val {} for conf key {} ", timeout, timeoutKey);
        this.timeout = Integer.parseInt(timeout);
    }

    @Override
    protected void doStart() {

        try {
            this.hbaseConfig = HBaseConfiguration.create(this.ctx.getConfiguration());
            String peerId = this.ctx.getPeerId();
            this.id = UUID.nameUUIDFromBytes(peerId.getBytes());
            startKafka();
            this.fileWatcherThread = Executors.newSingleThreadExecutor(new ThreadFactory() {

                @Override
                public Thread newThread(Runnable r) {
                    Thread th = new Thread(r, "sep-kafka-file-watcher-thread");
                    th.setDaemon(true);
                    return th;
                }
            });


            this.fileWatcherThread.execute(() -> {
                try {
                    //hardcoded for now, will need to split and pick from confPath later
                    setupWatcher("/usr/share/yak/", "reload.log");
                } catch (Exception e) {
                    logger.error("Failed to setup watcher");
                    throw new RuntimeException(e);
                }
            });
            notifyStarted();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed to start KafkaReplicationEndpoint ", e);
            notifyFailed(e);
        }

    }

    private void setupWatcher(String pathStr, String fileStr) throws Exception {
        logger.info("Setting up file watcher");
        final Path path = FileSystems.getDefault().getPath(pathStr);
        try (final WatchService watchService = FileSystems.getDefault().newWatchService()) {
            final WatchKey watchKey = path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            while (true) {
                final WatchKey wk = watchService.take();
                for (WatchEvent<?> event : wk.pollEvents()) {
                    // we only register "ENTRY_MODIFY" so the context is always a Path.
                    final Path changed = (Path) event.context();
                    logger.info("File Changed {}", changed.getFileName().toString());
                    if (changed.getFileName().toString().equals(fileStr)) {
                        logger.info("Got Modify event for {} attempt to stop and start Kafka", fileStr);
                        synchronized (lock) {
                            try {
                                logger.debug("accquired lock");
                                stopKafka();
                                startKafka();
                                logger.debug("release lock");
                            } catch (Exception e) {
                                logger.error("Failed to reboot Kafka ", e);
                            }
                        }
                    }
                }
                // reset the key
                boolean valid = wk.reset();
                if (!valid) {
                    logger.info("Key has been unregistered");
                    throw new Exception("Failed to register for future events");
                }
            }
        }
    }


    private void updatePublisherMap() {
        for (String cf : cfConfigMap.keySet()) {
            if (cf.trim().isEmpty()) {
                continue; // ignore blank space introduced
            }

            CFConfig cfConf = cfConfigMap.get(cf);

            Producer<byte[], byte[]> producer = buildProducer(cfConf, cf);
            if (producer != null) {
                this.publisherMap.putIfAbsent(cf, producer);
            }
        }
        logger.info("Configured brokers are :");
        for (String keys : this.publisherMap.keySet()) {
            logger.info(keys);
        }

    }

    private Producer<byte[], byte[]> buildProducer(CFConfig cfConfig,
                                                   String cf) {
        Map<String, Object> props = cfConfig.getKafkaConfig();
        logger.info("got props {} for conf cf {} ", props, cf);
        if (props == null){
            return null; // No Kafka replication configured
        }

        return new KafkaProducer<>(props);
    }

    private void stopKafka() throws Exception {
        logger.info("Stoping KafkaReplicationEndPoint");
        Iterator<java.util.Map.Entry<String, Producer<byte[], byte[]>>> it = publisherMap.entrySet().iterator();
        Exception th = null;
        while (it.hasNext()) {
            java.util.Map.Entry<String, Producer<byte[], byte[]>> entry = it.next();
            try {
                Producer<byte[], byte[]> prod = entry.getValue();
                prod.close();
                it.remove();
            } catch (Exception e) {
                logger.error("Failed to close producer {}", entry.getKey(), e);
                th = e;
            }
        }
        if (th != null)
            throw th;
        logger.info("Stopped Kafka Replication");
    }


    @Override
    protected void doStop() {
        try {
            stopKafka();
            notifyStopped();
        } catch (Exception e) {
            logger.error("Failed to stop kafkaReplicationEndpoint ", e);
            notifyFailed(e);
        }

    }

    public static void main(String[] args) {

        Path path = new File("/etc/fk-transact-yak/sep-conf.json").toPath();
        System.out.println(path.getFileName().toString());
        System.out.println(path.getParent().toString());

        // ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
        // "test", " 1-OD109625871071720000".getBytes(), "test".getBytes());
        // System.out.println("publishing to kafka keySr "
        // +new String(record.key())
        // +" "+( Utils.abs(Utils.murmur2(record.key())) % 16));

    }
}