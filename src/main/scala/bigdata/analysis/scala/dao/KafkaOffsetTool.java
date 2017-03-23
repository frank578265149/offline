package bigdata.analysis.scala.dao;

import bigdata.analysis.scala.utils.ConfigurationManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by think on 2016/11/21.
 */
public class KafkaOffsetTool {
    private static KafkaOffsetTool instance;
    final int TIMEOUT = 100000;
    final int BUFFERSIZE = 64 * 1024;

    private KafkaOffsetTool() {
    }

    public static synchronized KafkaOffsetTool getInstance() {
        if (instance == null) {
            instance = new KafkaOffsetTool();
        }
        return instance;
    }
    public Map<TopicAndPartition, Long> getLastOffset(String brokerList, List<String> topics,
                                                      String groupId) {

        Map<TopicAndPartition, Long> topicAndPartitionLongMap = Maps.newHashMap();

        Map<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerMap =
                KafkaOffsetTool.getInstance().findLeader(brokerList, topics);

        for (Map.Entry<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerEntry : topicAndPartitionBrokerMap
                .entrySet()) {
            // get leader broker
            BrokerEndPoint leaderBroker = topicAndPartitionBrokerEntry.getValue();

            SimpleConsumer simpleConsumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(),
                    TIMEOUT, BUFFERSIZE, groupId);

            long readOffset = getTopicAndPartitionLastOffset(simpleConsumer,
                    topicAndPartitionBrokerEntry.getKey(), groupId);

            topicAndPartitionLongMap.put(topicAndPartitionBrokerEntry.getKey(), readOffset);

        }

        return topicAndPartitionLongMap;

    }
    /**
     *
     * @param brokerList
     * @param topics
     * @param groupId
     * @return
     */
    public Map<TopicAndPartition, Long> getEarliestOffset(String brokerList, List<String> topics,
                                                          String groupId) {

        Map<TopicAndPartition, Long> topicAndPartitionLongMap = Maps.newHashMap();

        Map<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerMap =
                KafkaOffsetTool.getInstance().findLeader(brokerList, topics);

        for (Map.Entry<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerEntry : topicAndPartitionBrokerMap
                .entrySet()) {
            // get leader broker
            BrokerEndPoint leaderBroker = topicAndPartitionBrokerEntry.getValue();

            SimpleConsumer simpleConsumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(),
                    TIMEOUT, BUFFERSIZE, groupId);

            long readOffset = getTopicAndPartitionEarliestOffset(simpleConsumer,
                    topicAndPartitionBrokerEntry.getKey(), groupId);

            topicAndPartitionLongMap.put(topicAndPartitionBrokerEntry.getKey(), readOffset);

        }

        return topicAndPartitionLongMap;

    }

    /**
     * 得到所有的 TopicAndPartition
     *
     * @param brokerList
     * @param topics
     * @return topicAndPartitions
     */
    private Map<TopicAndPartition, BrokerEndPoint> findLeader(String brokerList, List<String> topics) {
        // get broker's url array
        String[] brokerUrlArray = getBorkerUrlFromBrokerList(brokerList);
        // get broker's port map
        Map<String, Integer> brokerPortMap = getPortFromBrokerList(brokerList);

        // create array list of TopicAndPartition
        Map<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerMap = Maps.newHashMap();
        for (String broker : brokerUrlArray) {

            SimpleConsumer consumer = null;
            try {
                // new instance of simple Consumer
                consumer = new SimpleConsumer(broker, brokerPortMap.get(broker), TIMEOUT, BUFFERSIZE,
                        "leaderLookup" + new Date().getTime());

                TopicMetadataRequest req = new TopicMetadataRequest(topics);

                TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();

                for (TopicMetadata item : metaData) {

                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        TopicAndPartition topicAndPartition =
                                new TopicAndPartition(item.topic(), part.partitionId());
                        topicAndPartitionBrokerMap.put(topicAndPartition, part.leader());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
        return topicAndPartitionBrokerMap;
    }
    /**
     * get earliest offset
     * @param consumer
     * @param topicAndPartition
     * @param clientName
     * @return
     */
    private long getTopicAndPartitionEarliestOffset(SimpleConsumer consumer,
                                                    TopicAndPartition topicAndPartition, String clientName) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
                new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                kafka.api.OffsetRequest.EarliestTime(), 1));

        OffsetRequest request = new OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                clientName);

        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out
                    .println("Error fetching data Offset Data the Broker. Reason: "
                            + response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()));
            return 0;
        }
        long[] offsets = response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
        return offsets[0];
    }
    private String[] getBorkerUrlFromBrokerList(String brokerlist) {
        String[] brokers = brokerlist.split(",");
        for (int i = 0; i < brokers.length; i++) {
            brokers[i] = brokers[i].split(":")[0];
        }
        return brokers;
    }

    /**
     * 得到broker url 与 其port 的映射关系
     *
     * @param brokerlist
     * @return
     */
    private Map<String, Integer> getPortFromBrokerList(String brokerlist) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        String[] brokers = brokerlist.split(",");
        for (String item : brokers) {
            String[] itemArr = item.split(":");
            if (itemArr.length > 1) {
                map.put(itemArr[0], Integer.parseInt(itemArr[1]));
            }
        }
        return map;
    }

    /**
     * get last offset
     * @param consumer
     * @param topicAndPartition
     * @param clientName
     * @return
     */
    private long getTopicAndPartitionLastOffset(SimpleConsumer consumer,
                                                TopicAndPartition topicAndPartition, String clientName) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
                new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                kafka.api.OffsetRequest.LatestTime(), 1));

        OffsetRequest request = new OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                clientName);

        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out
                    .println("Error fetching data Offset Data the Broker. Reason: "
                            + response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()));
            return 0;
        }
        long[] offsets = response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
        return offsets[0];
    }

    public static void main(String[] args) {
        List<String> topics = Lists.newArrayList();
        topics.add("bdcustinfoadd3");
        System.out.println("kafka.zk.topics.OScustinfoadd:" + ConfigurationManager.getProperty("kafka.zk.topics.OScustinfoadd"));
        System.out.println("kafka.zkservers:" + ConfigurationManager.getProperty("kafka.zkservers"));
//    topics.add("bugfix");
        Map<TopicAndPartition, Long> topicAndPartitionLongMap =
                KafkaOffsetTool.getInstance().getLastOffset("kafka01-qa.bi-report.v5q.cn:9092,kafka02-qa.bi-report.v5q.cn:9093", topics,
                        "streamST");

        for (Map.Entry<TopicAndPartition, Long> entry : topicAndPartitionLongMap.entrySet()) {
            System.out.println("last: [topic]"+ entry.getKey().topic() + "-[partition]"+ entry.getKey().partition() + ":[value]" + entry.getValue());
        }

        Map<TopicAndPartition, Long> topicAndPartitionLongMap2 =
                KafkaOffsetTool.getInstance().getEarliestOffset("kafka01-qa.bi-report.v5q.cn:9092,kafka02-qa.bi-report.v5q.cn:9093",
                        topics, "streamST");

        for (Map.Entry<TopicAndPartition, Long> entry : topicAndPartitionLongMap2.entrySet()) {
            System.out.println("early: [topic]"+ entry.getKey().topic() + "-[partition]"+ entry.getKey().partition() + ":[value]" + entry.getValue());
        }
    }

}
