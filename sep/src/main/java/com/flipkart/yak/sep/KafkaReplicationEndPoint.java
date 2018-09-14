package com.flipkart.yak.sep;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import org.apache.kafka.common.utils.Utils;
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
//    private boolean defaultPublisher;
    

    private int timeout = 1000; //future.getTimeout
    private static final String DEFAULT_STR = "default";
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public UUID getPeerUUID() {
        logger.debug("getPeer UUID {}", id);
        return this.id;
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
        try {
            logger.debug("replicate called");

            if(this.publisherMap.isEmpty() ){
                logger.debug("No replication configured, blindly acking");
                return true;
            }

            List<Entry> entries = replicateContext.getEntries();
            Map<String, List<Future<RecordMetadata>>> batchFutures = new HashMap<>(
                    this.publisherMap.size());

            for (String cf : this.publisherMap.keySet()) {
                List<Future<RecordMetadata>> records = new LinkedList<Future<RecordMetadata>>();
                batchFutures.put(cf, records);
            }

            for (Entry entry : entries) {
                WALKey key = entry.getKey();
                WALEdit val = entry.getEdit();
                TableName tableName = key.getTablename();
                SepTableName sepTable = SepTableName
                        .newBuilder()
                        .setNamespace(
                                ByteString.copyFrom(tableName.getNamespace()))
                                .setQualifier(ByteString.copyFrom(tableName.getName()))
                                .build();

                String topicPrefix = new StringBuilder()
                .append("yak")
                .append("_")
                .append(tableName.getNamespaceAsString())
                .append("_")
                .append(tableName.getQualifierAsString())
                .toString();
                for (Cell cell : val.getCells()) {
                    String qualifier = Bytes.toString(cell.getQualifierArray(),
                            cell.getQualifierOffset(),
                            cell.getQualifierLength());

                    String columnFamily =  Bytes.toString(cell.getFamilyArray(),
                            cell.getFamilyOffset(),
                            cell.getFamilyLength());

                   
                    CFConfig cfConf = null; ;
                    if (cfConfigMap.containsKey(columnFamily)) {
                       cfConf = cfConfigMap.get(columnFamily);
                    } else if(cfConfigMap.containsKey(DEFAULT_STR)) {
                    	   cfConf = cfConfigMap.get(DEFAULT_STR);
                    }else {
                    	    //ignore if not applicable.
                    		logger.warn("no cfConfig found");
                    		continue;
                    }
                    
                    String kafkaProducerLabel;
                    if (publisherMap.containsKey(columnFamily)) {
                        kafkaProducerLabel = columnFamily;
                     } else {
                    	 	kafkaProducerLabel = DEFAULT_STR;
                     }
                    
                    if(!cfConf.getWhiteListedQualifier().isEmpty() && !cfConf.getWhiteListedQualifier().contains(qualifier)) {
                    		logger.warn("ignoring not whitelisted qualifier");
                    		continue; // ignore not whitelisted if whiltelisted qualifier exists
                    }
                    
                    String topic = new StringBuilder()
                    .append(topicPrefix)
                    .append("_")
                    .append(columnFamily)
                    .append("_")
                    .append(qualifier).toString().toLowerCase();

                    if(cfConf.getQualifierToTopicNameMap().containsKey(qualifier)) {
                    		//override topicName as per config
                    		topic = cfConf.getQualifierToTopicNameMap().get(qualifier);
                    		logger.info("Changing topicName for qualifier {} to {}",qualifier, topic);
                    }
                   
                    if (publisherMap.containsKey(kafkaProducerLabel)) {
                        byte[] groupId = new byte[cell.getRowLength()];
                        System.arraycopy(cell.getRowArray(), cell.getRowOffset(), groupId, 0, groupId.length);
                        logger.debug("pushing to cf {} rowKey {}", kafkaProducerLabel, new String(groupId));
                        
                        
                        SepMessage msg = SepMessageProto.SepMessage
                                .newBuilder()
                                .setRow(ByteString.copyFrom(cell.getRowArray(),
                                        cell.getRowOffset(),
                                        cell.getRowLength()))
                                        .setTable(sepTable)
                                        .setColumnfamily(ByteString.copyFrom(cell.getFamilyArray(),
                                        		cell.getFamilyOffset(), cell.getFamilyLength()))
                                        .setQualifier(ByteString.copyFrom(cell.getQualifierArray(),
                                        		cell.getQualifierOffset(), cell.getQualifierLength()))
                                        .setTimestamp(key.getWriteTime())
                                        .setValue(
                                                ByteString.copyFrom(
                                                        cell.getValueArray(),
                                                        cell.getValueOffset(),
                                                        cell.getValueLength())).build();
                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                                topic, groupId, msg.toByteArray());
                        //                        logger.info("groupId {}, msgRow {}", new String(groupId),msg.getRow());
                        //                        logger.info("publishing to kafka keySr {}, partition {},", new String(record.key()), Utils.abs(Utils.murmur2(record.key())) % 16);
                        Future<RecordMetadata> future = this.publisherMap.get(
                                kafkaProducerLabel).send(record);
                        batchFutures.get(kafkaProducerLabel).add(future);
                    } else {
                        logger.warn(
                                "No kafka broker configured for this kafkaProducer {}", kafkaProducerLabel);
                        continue;
                    }
                }

            }

            logger.debug("Flushing futures ");
            // flush all
            for (String cf : batchFutures.keySet()) {
                List<Future<RecordMetadata>> lst = batchFutures.get(cf);
                for (Future<RecordMetadata> f : lst) {
                    f.get(timeout,TimeUnit.MILLISECONDS); // this will throw exception if failed
                    // resulting in retries of all
                }
            }
            logger.debug("Flushed futures");
            return true;
        } catch (Throwable e) {
            logger.error("Error in replicate method call ", e);
            return false;
        }
    }

    @Override
    protected void doStart() {
        logger.info("Starting KafkaReplicationEndPoint ");
        try {
            Configuration hbaseConfig = HBaseConfiguration.create(this.ctx
                    .getConfiguration());
            String peerId = this.ctx.getPeerId();
            this.id = UUID.nameUUIDFromBytes(peerId.getBytes());
            this.publisherMap = new HashMap<>();
            this.cfConfigMap  = new HashMap<>();
            String confPath = hbaseConfig.get("sep.kafka.config.path");
          
            
            if(confPath != null) {
            	  SepConfig conf = mapper.readValue(new File(confPath),
                  		SepConfig.class);
            	  this.cfConfigMap = conf.getCfConfig();
            	  updatePublisherMap();
            }else {
            		logger.info("No config.path configured for kafka push");
            }
            
            //added to ensure timeout happens
            String timeoutKey = "sep.kafka.timeout";
            String timeout = hbaseConfig.get(timeoutKey,"1000");
            logger.info("got val {} for conf key {} ", timeout, timeoutKey);
            this.timeout  = Integer.parseInt(timeout);
            // setup kafka producer
           
            notifyStarted();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed to start KafkaReplicationEndpoint ", e);
            notifyFailed(e);
        }

    }
    
    

    private void updatePublisherMap() {
         for (String cf : cfConfigMap.keySet()) {
             if (cf.trim().isEmpty()){
                 continue; // ignore blank space introduced
             }
             
             CFConfig cfConf = cfConfigMap.get(cf);
             
             Producer<byte[], byte[]> producer = buildProducer(cfConf, cf);
             if(producer != null){
                 this.publisherMap.put(cf, producer);
             }
         }
         logger.info("Configured brokers are :");
         for (String keys : this.publisherMap.keySet()) {
             logger.info(keys);
         }
         
	}

	private Producer<byte[], byte[]> buildProducer(CFConfig cfConfig,
            String cf) {
		Map<String,Object> props = cfConfig.getKafkaConfig();
        logger.info("got props {} for conf cf {} ", props, cf);
        if (props == null){
            return null; // No Kafka replication configured
        }
        
        return new KafkaProducer<>(props);
    }

    @Override
    protected void doStop() {
        logger.info("Stopped KafkaReplicationEndPoint");
        try {
            Iterator<java.util.Map.Entry<String, Producer<byte[], byte[]>>> it = publisherMap
                    .entrySet().iterator();
            Exception th = null;
            while (it.hasNext()) {
                java.util.Map.Entry<String, Producer<byte[], byte[]>> entry = it
                        .next();
                try {
                    Producer<byte[], byte[]> prod = entry.getValue();
                    prod.close();
                    it.remove();
                } catch (Exception e) {
                    logger.error("Failed to close producer {}", entry.getKey(),
                            e);
                    th = e;
                }
            }
            if (th != null)
                throw th;
            notifyStopped();
        } catch (Exception e) {
            logger.error("Failed to stop kafkaReplicationEndpoint ", e);
            notifyFailed(e);
        }

    }

    public static void main(String[] args) {
        
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                "test", " 1-OD109625871071720000".getBytes(), "test".getBytes());
        System.out.println("publishing to kafka keySr "
                +new String(record.key())
                +" "+( Utils.abs(Utils.murmur2(record.key())) % 16));
    	
    	
    		
    }
}
