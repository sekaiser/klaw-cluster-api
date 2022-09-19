package io.aiven.klaw.clusterapi.services;

import io.aiven.klaw.clusterapi.models.ClusterResponseStatus;
import io.aiven.klaw.clusterapi.models.KafkaClustersType;
import io.aiven.klaw.clusterapi.utils.ClusterApiUtils;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Service
@Slf4j
public class SchemaService {
  public static final String SCHEMA_REGISTRY_CONTENT_TYPE =
      "application/vnd.schemaregistry.v1+json";

  @Value("${kafkawize.schemaregistry.compatibility.default:BACKWARD}")
  private String defaultSchemaCompatibility;

  final ClusterApiUtils clusterApiUtils;

  public SchemaService(ClusterApiUtils clusterApiUtils) {
    this.clusterApiUtils = clusterApiUtils;
  }

  public synchronized String registerSchema(
      String topicName, String schema, String environmentVal, String protocol) {
    try {
      log.info(
          "Into register schema request TopicName:{} Env:{} Protocol:{}",
          topicName,
          environmentVal,
          protocol);
      if (environmentVal == null) return "Cannot retrieve SchemaRegistry Url";

      //            set default compatibility
      //            setSchemaCompatibility(environmentVal, topicName, false, protocol);
      String suffixUrl = environmentVal + "/subjects/" + topicName + "-value/versions";
      Pair<String, RestTemplate> reqDetails =
          clusterApiUtils.getRequestDetails(suffixUrl, protocol, KafkaClustersType.SCHEMA_REGISTRY);

      Map<String, String> params = new HashMap<>();
      params.put("schema", schema);

      HttpHeaders headers = new HttpHeaders();
      headers.set("Content-Type", SCHEMA_REGISTRY_CONTENT_TYPE);
      HttpEntity<Map<String, String>> request = new HttpEntity<>(params, headers);
      ResponseEntity<String> responseNew =
          reqDetails.getRight().postForEntity(reqDetails.getLeft(), request, String.class);

      String updateTopicReqStatus = responseNew.getBody();
      log.info(responseNew.getBody());

      return updateTopicReqStatus;
    } catch (Exception e) {
      log.error(e.getMessage());
      if (((HttpClientErrorException.Conflict) e).getStatusCode().value() == 409) {
        return "Schema being registered is incompatible with an earlier schema";
      }
      return "Failure in registering schema.";
    }
  }

  public Map<Integer, Map<String, Object>> getSchema(
      String environmentVal, String protocol, String topicName) {
    try {
      log.info("Into getSchema request {} {} {}", topicName, environmentVal, protocol);
      if (environmentVal == null) return null;

      List<Integer> versionsList = getSchemaVersions(environmentVal, topicName, protocol);
      String schemaCompatibility = getSchemaCompatibility(environmentVal, topicName, protocol);
      Map<Integer, Map<String, Object>> allSchemaObjects = new TreeMap<>();

      if (versionsList != null) {
        for (Integer schemaVersion : versionsList) {
          String suffixUrl =
              environmentVal + "/subjects/" + topicName + "-value/versions/" + schemaVersion;
          Pair<String, RestTemplate> reqDetails =
              clusterApiUtils.getRequestDetails(
                  suffixUrl, protocol, KafkaClustersType.SCHEMA_REGISTRY);

          Map<String, String> params = new HashMap<>();

          ResponseEntity<HashMap> responseNew =
              reqDetails.getRight().getForEntity(reqDetails.getLeft(), HashMap.class, params);
          Map<String, Object> schemaResponse = responseNew.getBody();
          if (schemaResponse != null) schemaResponse.put("compatibility", schemaCompatibility);

          log.info(Objects.requireNonNull(responseNew.getBody()).toString());
          allSchemaObjects.put(schemaVersion, schemaResponse);
        }
      }

      return allSchemaObjects;
    } catch (Exception e) {
      log.error("Error from getSchema : " + e.getMessage());
      return new TreeMap<>();
    }
  }

  private List<Integer> getSchemaVersions(
      String environmentVal, String topicName, String protocol) {
    try {
      log.info("Into getSchema versions {} {}", topicName, environmentVal);
      if (environmentVal == null) return null;

      String suffixUrl = environmentVal + "/subjects/" + topicName + "-value/versions";
      Pair<String, RestTemplate> reqDetails =
          clusterApiUtils.getRequestDetails(suffixUrl, protocol, KafkaClustersType.SCHEMA_REGISTRY);

      Map<String, String> params = new HashMap<>();

      ResponseEntity<ArrayList> responseList =
          reqDetails.getRight().getForEntity(reqDetails.getLeft(), ArrayList.class, params);
      log.info("Schema versions " + responseList);
      return responseList.getBody();
    } catch (Exception e) {
      log.error("Error in getting versions " + e.getMessage());
      return new ArrayList<>();
    }
  }

  private String getSchemaCompatibility(String environmentVal, String topicName, String protocol) {
    try {
      log.info("Into getSchema compatibility {} {}", topicName, environmentVal);
      if (environmentVal == null) return null;

      String suffixUrl = environmentVal + "/config/" + topicName + "-value";
      Pair<String, RestTemplate> reqDetails =
          clusterApiUtils.getRequestDetails(suffixUrl, protocol, KafkaClustersType.SCHEMA_REGISTRY);

      Map<String, String> params = new HashMap<>();

      ResponseEntity<HashMap> responseList =
          reqDetails.getRight().getForEntity(reqDetails.getLeft(), HashMap.class, params);
      log.info("Schema compatibility " + responseList);
      return (String) responseList.getBody().get("compatibilityLevel");
    } catch (Exception e) {
      log.error("Error in getting schema compatibility " + e.getMessage());
      return "NOT SET";
    }
  }

  private boolean setSchemaCompatibility(
      String environmentVal, String topicName, boolean isForce, String protocol) {
    try {
      log.info("Into setSchema compatibility {} {}", topicName, environmentVal);
      if (environmentVal == null) return false;

      String suffixUrl = environmentVal + "/config/" + topicName + "-value";
      Pair<String, RestTemplate> reqDetails =
          clusterApiUtils.getRequestDetails(suffixUrl, protocol, KafkaClustersType.SCHEMA_REGISTRY);

      Map<String, String> params = new HashMap<>();
      if (isForce) params.put("compatibility", "NONE");
      else params.put("compatibility", defaultSchemaCompatibility);

      HttpHeaders headers = new HttpHeaders();
      headers.set("Content-Type", SCHEMA_REGISTRY_CONTENT_TYPE);
      HttpEntity<Map<String, String>> request = new HttpEntity<>(params, headers);
      reqDetails.getRight().put(reqDetails.getLeft(), request, String.class);
      return true;
    } catch (Exception e) {
      log.error("Error in setting schema compatibility " + e.getMessage());
      return false;
    }
  }

  protected String getSchemaRegistryStatus(String environmentVal, String protocol) {

    String suffixUrl = environmentVal + "/subjects";
    Pair<String, RestTemplate> reqDetails =
        clusterApiUtils.getRequestDetails(suffixUrl, protocol, KafkaClustersType.SCHEMA_REGISTRY);

    Map<String, String> params = new HashMap<>();

    try {
      reqDetails.getRight().getForEntity(reqDetails.getLeft(), Object.class, params);
      return ClusterResponseStatus.ONLINE.value;
    } catch (RestClientException e) {
      e.printStackTrace();
      return ClusterResponseStatus.OFFLINE.value;
    }
  }
}
