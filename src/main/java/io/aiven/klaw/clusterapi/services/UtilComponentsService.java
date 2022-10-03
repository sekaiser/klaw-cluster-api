package io.aiven.klaw.clusterapi.services;

import io.aiven.klaw.clusterapi.models.ClusterResponseStatus;
import io.aiven.klaw.clusterapi.utils.ClusterApiUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class UtilComponentsService {

  Environment env;

  ClusterApiUtils clusterApiUtils;

  SchemaService schemaService;

  KafkaConnectService kafkaConnectService;

  public UtilComponentsService() {}

  public UtilComponentsService(Environment env, ClusterApiUtils clusterApiUtils) {
    this.env = env;
    this.clusterApiUtils = clusterApiUtils;
  }

  @Autowired
  public UtilComponentsService(
      Environment env,
      ClusterApiUtils clusterApiUtils,
      SchemaService schemaService,
      KafkaConnectService kafkaConnectService) {
    this.env = env;
    this.clusterApiUtils = clusterApiUtils;
    this.schemaService = schemaService;
    this.kafkaConnectService = kafkaConnectService;
  }

  //    public String reloadTruststore(String protocol, String clusterName){
  //        getAdminClient.removeSSLElementFromAdminClientMap(protocol, clusterName);
  //        return ResultType.SUCCESS.value;
  //    }

  public String getStatus(
      String environment, String protocol, String clusterName, String clusterType) {
    log.info("getStatus {} {}", environment, protocol);
    switch (clusterType) {
      case "kafka":
        return getStatusKafka(environment, protocol, clusterName);
      case "schemaregistry":
        return schemaService.getSchemaRegistryStatus(environment, protocol);
      case "kafkaconnect":
        return kafkaConnectService.getKafkaConnectStatus(environment, protocol);
      default:
        return ClusterResponseStatus.OFFLINE.value;
    }
  }

  private String getStatusKafka(String environment, String protocol, String clusterName) {
    try {
      AdminClient client = clusterApiUtils.getAdminClient(environment, protocol, clusterName);
      if (client != null) {
        return ClusterResponseStatus.ONLINE.value;
      } else return ClusterResponseStatus.OFFLINE.value;

    } catch (Exception e) {
      e.printStackTrace();
      log.error(e.getMessage());
      return ClusterResponseStatus.OFFLINE.value;
    }
  }
}
