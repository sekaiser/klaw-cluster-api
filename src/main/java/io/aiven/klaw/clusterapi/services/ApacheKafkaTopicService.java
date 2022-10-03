package io.aiven.klaw.clusterapi.services;

import io.aiven.klaw.clusterapi.models.ClusterTopicRequest;
import io.aiven.klaw.clusterapi.models.ResultType;
import io.aiven.klaw.clusterapi.utils.ClusterApiUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ApacheKafkaTopicService {

  private static final long TIME_OUT_SECS_FOR_TOPICS = 5;

  Environment env;

  @Autowired ClusterApiUtils clusterApiUtils;

  public ApacheKafkaTopicService() {}

  public ApacheKafkaTopicService(Environment env) {
    this.env = env;
  }

  public synchronized Set<HashMap<String, String>> loadTopics(
      String environment, String protocol, String clusterName) throws Exception {
    log.info("loadTopics {} {}", environment, protocol);
    AdminClient client = clusterApiUtils.getAdminClient(environment, protocol, clusterName);
    Set<HashMap<String, String>> topics = new HashSet<>();
    if (client == null) throw new Exception("Cannot connect to cluster.");

    ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
    listTopicsOptions = listTopicsOptions.listInternal(false);

    ListTopicsResult topicsResult = client.listTopics(listTopicsOptions);

    try {
      DescribeTopicsResult s = client.describeTopics(new ArrayList<>(topicsResult.names().get()));
      Map<String, TopicDescription> topicDesc =
          s.all().get(TIME_OUT_SECS_FOR_TOPICS, TimeUnit.SECONDS);
      Set<String> keySet = topicDesc.keySet();
      keySet.remove("_schemas");
      List<String> lstK = new ArrayList<>(keySet);
      HashMap<String, String> hashMap;
      for (String topicName : lstK) {
        hashMap = new HashMap<>();
        hashMap.put("topicName", topicName);
        hashMap.put(
            "replicationFactor",
            "" + topicDesc.get(topicName).partitions().get(0).replicas().size());
        hashMap.put("partitions", "" + topicDesc.get(topicName).partitions().size());
        if (!topicName.startsWith("_confluent")) topics.add(hashMap);
      }

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.error(e.getMessage());
    }
    return topics;
  }

  public synchronized String createTopic(ClusterTopicRequest clusterTopicRequest)
      //      String name,
      //                                         String partitions,
      //                                         String replicationFactor,
      //                                         String environment,
      //                                         String protocol,
      //                                         String clusterName)
      throws Exception {

    log.info(
        "createTopic Name: {} Partitions:{} Replication factor:{} Environment:{} Protocol:{} clusterName:{}",
        clusterTopicRequest.getTopicName(),
        clusterTopicRequest.getPartitions(),
        clusterTopicRequest.getReplicationFactor(),
        clusterTopicRequest.getEnv(),
        clusterTopicRequest.getProtocol(),
        clusterTopicRequest.getClusterName());

    AdminClient client;
    try {
      client =
          clusterApiUtils.getAdminClient(
              clusterTopicRequest.getEnv(),
              clusterTopicRequest.getProtocol(),
              clusterTopicRequest.getClusterName());
      if (client == null) throw new Exception("Cannot connect to cluster.");

      NewTopic topic =
          new NewTopic(
              clusterTopicRequest.getTopicName(),
              clusterTopicRequest.getPartitions(),
              clusterTopicRequest.getReplicationFactor());

      CreateTopicsResult result = client.createTopics(Collections.singletonList(topic));
      result
          .values()
          .get(clusterTopicRequest.getTopicName())
          .get(TIME_OUT_SECS_FOR_TOPICS, TimeUnit.SECONDS);
    } catch (KafkaException e) {
      String errorMessage = "Invalid properties: ";
      log.error(errorMessage, e);
      throw e;
    } catch (NumberFormatException e) {
      String errorMessage = "Invalid replica assignment string";
      log.error(errorMessage, e);
      throw e;
    } catch (ExecutionException | InterruptedException e) {
      String errorMessage;
      if (e instanceof ExecutionException) {
        errorMessage = e.getCause().getMessage();
      } else {
        Thread.currentThread().interrupt();
        errorMessage = e.getMessage();
      }
      log.error("Unable to create topic {}, {}", clusterTopicRequest.getTopicName(), errorMessage);
      throw e;
    } catch (Exception e) {
      log.error(e.getMessage());
      e.printStackTrace();
      throw e;
    }

    return ResultType.SUCCESS.value;
  }

  public synchronized String updateTopic(
      String topicName,
      String partitions,
      String replicationFactor,
      String environment,
      String protocol,
      String clusterName)
      throws Exception {

    log.info(
        "updateTopic Name: {} Partitions:{} Replication factor:{} Environment:{} Protocol:{} clusterName:{}",
        topicName,
        partitions,
        replicationFactor,
        environment,
        protocol,
        clusterName);

    AdminClient client = clusterApiUtils.getAdminClient(environment, protocol, clusterName);

    if (client == null) throw new Exception("Cannot connect to cluster.");

    DescribeTopicsResult describeTopicsResult =
        client.describeTopics(Collections.singleton(topicName));
    TopicDescription result =
        describeTopicsResult.all().get(TIME_OUT_SECS_FOR_TOPICS, TimeUnit.SECONDS).get(topicName);

    if (result.partitions().size() > Integer.parseInt(partitions)) {
      // delete topic and recreate
      deleteTopic(topicName, environment, protocol, clusterName);
      createTopic(
          ClusterTopicRequest.builder()
              .env(environment)
              .protocol(protocol)
              .topicName(topicName)
              .partitions(Integer.parseInt(partitions))
              .replicationFactor(Short.parseShort(replicationFactor))
              .clusterName(clusterName)
              .build());
    } else {
      // update config
      //            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC,
      // topicName);
      //            ConfigEntry retentionEntry = new ConfigEntry(TopicConfig., "60000");
      //            Map<ConfigResource, Collection<AlterConfigOp>> updateConfig = new HashMap<>();
      //            AlterConfigOp alterConfigOp = new AlterConfigOp();
      //            client.incrementalAlterConfigs(updateConfig);

      Map<String, NewPartitions> newPartitionSet = new HashMap<>();
      newPartitionSet.put(topicName, NewPartitions.increaseTo(Integer.parseInt(partitions)));

      client.createPartitions(newPartitionSet);
    }

    return ResultType.SUCCESS.value;
  }

  public synchronized void deleteTopic(
      String topicName, String environment, String protocol, String clusterName) throws Exception {

    log.info(
        "deleteTopic Topic name:{} Env:{} Protocol:{} clusterName:{}",
        topicName,
        environment,
        protocol,
        clusterName);

    AdminClient client;
    try {
      client = clusterApiUtils.getAdminClient(environment, protocol, clusterName);
      if (client == null) throw new Exception("Cannot connect to cluster.");

      DeleteTopicsResult result = client.deleteTopics(Collections.singletonList(topicName));
      result.values().get(topicName).get(TIME_OUT_SECS_FOR_TOPICS, TimeUnit.SECONDS);
    } catch (KafkaException e) {
      String errorMessage = "Invalid properties: ";
      log.error(errorMessage, e);
      throw e;
    } catch (ExecutionException | InterruptedException e) {
      String errorMessage;
      if (e instanceof ExecutionException) {
        errorMessage = e.getCause().getMessage();
      } else {
        Thread.currentThread().interrupt();
        errorMessage = e.getMessage();
      }
      log.error("Unable to delete topic {}, {}", topicName, errorMessage);

      throw e;
    } catch (Exception e) {
      log.error(e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }
}
