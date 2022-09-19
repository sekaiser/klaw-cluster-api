package io.aiven.klaw.clusterapi.services;

import io.aiven.klaw.clusterapi.models.ClusterResponseStatus;
import io.aiven.klaw.clusterapi.models.KafkaClustersType;
import io.aiven.klaw.clusterapi.models.ResultType;
import io.aiven.klaw.clusterapi.utils.ClusterApiUtils;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Service
@Slf4j
public class KafkaConnectService {

  final ClusterApiUtils clusterApiUtils;

  public KafkaConnectService(ClusterApiUtils clusterApiUtils) {
    this.clusterApiUtils = clusterApiUtils;
  }

  public HashMap<String, String> deleteConnector(
      String environmentVal, String protocol, String connectorName) {
    log.info("Into deleteConnector {} {} {}", environmentVal, connectorName, protocol);
    HashMap<String, String> result = new HashMap<>();

    if (environmentVal == null) return null;

    String suffixUrl = environmentVal + "/connectors/" + connectorName;
    Pair<String, RestTemplate> reqDetails =
        clusterApiUtils.getRequestDetails(suffixUrl, protocol, KafkaClustersType.KAFKA_CONNECT);

    try {
      reqDetails.getRight().delete(reqDetails.getLeft(), String.class);
    } catch (RestClientException e) {
      log.error("Error in deleting connector " + e.toString());
      result.put("result", ResultType.ERROR.value);
      result.put("errorText", e.toString().replaceAll("\"", ""));
      return result;
    }
    result.put("result", ResultType.SUCCESS.value);
    return result;
  }

  public Map<String, String> updateConnector(
      String environmentVal, String protocol, String connectorName, String connectorConfig) {
    log.info(
        "Into updateConnector {} {} {} {}",
        environmentVal,
        connectorName,
        connectorConfig,
        protocol);
    Map<String, String> result = new HashMap<>();

    if (environmentVal == null) return null;

    String suffixUrl = environmentVal + "/connectors/" + connectorName + "/config";
    Pair<String, RestTemplate> reqDetails =
        clusterApiUtils.getRequestDetails(suffixUrl, protocol, KafkaClustersType.KAFKA_CONNECT);

    HttpHeaders headers = new HttpHeaders();
    headers.set("Content-Type", "application/json");

    HttpEntity<String> request = new HttpEntity<>(connectorConfig, headers);

    try {
      reqDetails.getRight().put(reqDetails.getLeft(), request, String.class);
    } catch (RestClientException e) {
      log.error("Error in updating connector " + e.toString());
      result.put("result", "error");
      result.put("errorText", e.toString().replaceAll("\"", ""));
      return result;
    }
    result.put("result", ResultType.SUCCESS.value);

    return result;
  }

  public Map<String, String> postNewConnector(
      String environmentVal, String protocol, String connectorConfig) {
    log.info("Into postNewConnector {} {} {}", environmentVal, connectorConfig, protocol);
    Map<String, String> result = new HashMap<>();
    if (environmentVal == null) return null;

    String suffixUrl = environmentVal + "/connectors";
    Pair<String, RestTemplate> reqDetails =
        clusterApiUtils.getRequestDetails(suffixUrl, protocol, KafkaClustersType.KAFKA_CONNECT);

    HttpHeaders headers = new HttpHeaders();
    headers.set("Content-Type", "application/json");

    HttpEntity<String> request = new HttpEntity<>(connectorConfig, headers);
    ResponseEntity<String> responseNew;
    try {
      responseNew =
          reqDetails.getRight().postForEntity(reqDetails.getLeft(), request, String.class);
    } catch (RestClientException e) {
      log.error("Error in registering new connector " + e.toString());
      result.put("result", ResultType.ERROR.value);
      result.put("errorText", e.toString().replaceAll("\"", ""));
      return result;
    }
    if (responseNew.getStatusCodeValue() == 201) {
      result.put("result", ResultType.SUCCESS.value);
    } else {
      result.put("result", ResultType.FAILURE.value);
    }
    return result;
  }

  public List<String> getConnectors(String environmentVal, String protocol) {
    try {
      log.info("Into getConnectors {} {}", environmentVal, protocol);
      if (environmentVal == null) return null;

      String suffixUrl = environmentVal + "/connectors";
      Pair<String, RestTemplate> reqDetails =
          clusterApiUtils.getRequestDetails(suffixUrl, protocol, KafkaClustersType.KAFKA_CONNECT);

      Map<String, String> params = new HashMap<>();
      ResponseEntity<List> responseList =
          reqDetails.getRight().getForEntity(reqDetails.getLeft(), List.class, params);

      log.info("connectors list " + responseList);
      return responseList.getBody();
    } catch (Exception e) {
      log.error("Error in getting connectors " + e.getMessage());
      return new ArrayList<>();
    }
  }

  public LinkedHashMap<String, Object> getConnectorDetails(
      String connector, String environmentVal, String protocol) {
    try {
      log.info("Into getConnectorDetails {} {}", environmentVal, protocol);
      if (environmentVal == null) return null;

      String suffixUrl = environmentVal + "/connectors" + "/" + connector;
      Pair<String, RestTemplate> reqDetails =
          clusterApiUtils.getRequestDetails(suffixUrl, protocol, KafkaClustersType.KAFKA_CONNECT);

      Map<String, String> params = new HashMap<>();

      ResponseEntity<LinkedHashMap> responseList =
          reqDetails.getRight().getForEntity(reqDetails.getLeft(), LinkedHashMap.class, params);
      log.info("connectors list " + responseList);

      return responseList.getBody();
    } catch (Exception e) {
      log.error("Error in getting connector detail " + e.getMessage());
      return new LinkedHashMap<>();
    }
  }

  protected String getKafkaConnectStatus(String environment, String protocol) {
    String suffixUrl = environment + "/connectors";
    Pair<String, RestTemplate> reqDetails =
        clusterApiUtils.getRequestDetails(suffixUrl, protocol, KafkaClustersType.KAFKA_CONNECT);
    Map<String, String> params = new HashMap<String, String>();

    try {
      ResponseEntity<Object> responseNew =
          reqDetails.getRight().getForEntity(reqDetails.getLeft(), Object.class, params);
      return ClusterResponseStatus.ONLINE.value;
    } catch (RestClientException e) {
      e.printStackTrace();
      return ClusterResponseStatus.OFFLINE.value;
    }
  }
}
