package io.aiven.klaw.clusterapi.controller;

import io.aiven.klaw.clusterapi.models.AclsNativeType;
import io.aiven.klaw.clusterapi.models.ApiResponse;
import io.aiven.klaw.clusterapi.models.ClusterAclRequest;
import io.aiven.klaw.clusterapi.models.ClusterTopicRequest;
import io.aiven.klaw.clusterapi.services.AivenApiService;
import io.aiven.klaw.clusterapi.services.ApacheKafkaAclService;
import io.aiven.klaw.clusterapi.services.ApacheKafkaTopicService;
import io.aiven.klaw.clusterapi.services.MonitoringService;
import io.aiven.klaw.clusterapi.services.SchemaService;
import io.aiven.klaw.clusterapi.services.UtilComponentsService;
import java.util.*;
import javax.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/topics")
@Slf4j
public class ClusterApiController {

  @Autowired UtilComponentsService utilComponentsService;

  @Autowired ApacheKafkaAclService apacheKafkaAclService;

  @Autowired ApacheKafkaTopicService apacheKafkaTopicService;

  @Autowired SchemaService schemaService;

  @Autowired MonitoringService monitoringService;

  @Autowired AivenApiService aivenApiService;

  @RequestMapping(
      value = "/getApiStatus",
      method = RequestMethod.GET,
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<String> getApiStatus() {
    return new ResponseEntity<>("ONLINE", HttpStatus.OK);
  }

  //    @RequestMapping(value = "/reloadTruststore/{protocol}/{clusterName}", method =
  // RequestMethod.GET,produces = {MediaType.APPLICATION_JSON_VALUE})
  //    public ResponseEntity<String> reloadTruststore(@PathVariable String protocol,
  //                                                   @PathVariable String clusterName){
  //        return new ResponseEntity<>(manageKafkaComponents.reloadTruststore(protocol,
  // clusterName), HttpStatus.OK);
  //    }

  @RequestMapping(
      value = "/getStatus/{bootstrapServers}/{protocol}/{clusterName}/{clusterType}",
      method = RequestMethod.GET,
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<String> getStatus(
      @PathVariable String bootstrapServers,
      @PathVariable String protocol,
      @PathVariable String clusterName,
      @PathVariable String clusterType) {
    String envStatus =
        utilComponentsService.getStatus(bootstrapServers, protocol, clusterName, clusterType);

    return new ResponseEntity<>(envStatus, HttpStatus.OK);
  }

  @RequestMapping(
      value = "/getTopics/{bootstrapServers}/{protocol}/{clusterName}",
      method = RequestMethod.GET,
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<Set<HashMap<String, String>>> getTopics(
      @PathVariable String bootstrapServers,
      @PathVariable String protocol,
      @PathVariable String clusterName)
      throws Exception {
    Set<HashMap<String, String>> topics =
        apacheKafkaTopicService.loadTopics(bootstrapServers, protocol, clusterName);
    return new ResponseEntity<>(topics, HttpStatus.OK);
  }

  @RequestMapping(
      value =
          "/getAcls/{bootstrapServers}/{aclsNativeType}/{protocol}/{clusterName}/{projectName}/{serviceName}",
      method = RequestMethod.GET,
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<Set<Map<String, String>>> getAcls(
      @PathVariable String bootstrapServers,
      @PathVariable String protocol,
      @PathVariable String clusterName,
      @PathVariable String aclsNativeType,
      @PathVariable String projectName,
      @PathVariable String serviceName)
      throws Exception {
    Set<Map<String, String>> acls;
    if (AclsNativeType.NATIVE.name().equals(aclsNativeType))
      acls = apacheKafkaAclService.loadAcls(bootstrapServers, protocol, clusterName);
    else acls = aivenApiService.listAcls(projectName, serviceName);

    return new ResponseEntity<>(acls, HttpStatus.OK);
  }

  @RequestMapping(
      value = "/getSchema/{bootstrapServers}/{protocol}/{clusterName}/{topicName}",
      method = RequestMethod.GET,
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<Map<Integer, Map<String, Object>>> getSchema(
      @PathVariable String bootstrapServers,
      @PathVariable String protocol,
      @PathVariable String topicName,
      @PathVariable String clusterName) {
    Map<Integer, Map<String, Object>> schema =
        schemaService.getSchema(bootstrapServers, protocol, topicName);

    return new ResponseEntity<>(schema, HttpStatus.OK);
  }

  @RequestMapping(
      value =
          "/getConsumerOffsets/{bootstrapServers}/{protocol}/{clusterName}/{consumerGroupId}/{topicName}",
      method = RequestMethod.GET,
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<List<HashMap<String, String>>> getConsumerOffsets(
      @PathVariable String bootstrapServers,
      @PathVariable String protocol,
      @PathVariable String clusterName,
      @PathVariable String consumerGroupId,
      @PathVariable String topicName)
      throws Exception {
    List<HashMap<String, String>> consumerOffsetDetails =
        monitoringService.getConsumerGroupDetails(
            consumerGroupId, topicName, bootstrapServers, protocol, clusterName);

    return new ResponseEntity<>(consumerOffsetDetails, HttpStatus.OK);
  }

  @PostMapping(value = "/createTopics")
  public ResponseEntity<String> createTopics(
      @RequestBody @Valid ClusterTopicRequest clusterTopicRequest) {
    try {
      log.info("createTopics clusterTopicRequest {}", clusterTopicRequest);
      apacheKafkaTopicService.createTopic(clusterTopicRequest);
    } catch (Exception e) {
      log.error(e.getMessage());
      return new ResponseEntity<>("failure " + e, HttpStatus.OK);
    }

    return new ResponseEntity<>("success", HttpStatus.OK);
  }

  @PostMapping(value = "/updateTopics")
  public ResponseEntity<String> updateTopics(
      @RequestBody MultiValueMap<String, String> topicRequest) {
    try {
      apacheKafkaTopicService.updateTopic(
          topicRequest.get("topicName").get(0),
          topicRequest.get("partitions").get(0),
          topicRequest.get("rf").get(0),
          topicRequest.get("env").get(0),
          topicRequest.get("protocol").get(0),
          topicRequest.get("clusterName").get(0));
    } catch (Exception e) {
      log.error(e.getMessage());
      return new ResponseEntity<>("failure " + e, HttpStatus.OK);
    }

    return new ResponseEntity<>("success", HttpStatus.OK);
  }

  @PostMapping(value = "/deleteTopics")
  public ResponseEntity<String> deleteTopics(
      @RequestBody MultiValueMap<String, String> topicRequest) {
    try {
      apacheKafkaTopicService.deleteTopic(
          topicRequest.get("topicName").get(0),
          topicRequest.get("env").get(0),
          topicRequest.get("protocol").get(0),
          topicRequest.get("clusterName").get(0));
    } catch (Exception e) {
      log.error(e.getMessage());
      return new ResponseEntity<>("failure " + e, HttpStatus.OK);
    }

    return new ResponseEntity<>("success", HttpStatus.OK);
  }

  @PostMapping(value = "/createAcls")
  public ResponseEntity<ApiResponse> createAcls(
      @RequestBody @Valid ClusterAclRequest clusterAclRequest) {

    Map<String, String> resultMap = new HashMap<>();
    String result;
    try {
      if (AclsNativeType.NATIVE.name().equals(clusterAclRequest.getAclNativeType())) {
        if ("Producer".equals(clusterAclRequest.getAclType()))
          result = apacheKafkaAclService.updateProducerAcl(clusterAclRequest);
        else result = apacheKafkaAclService.updateConsumerAcl(clusterAclRequest);
        return new ResponseEntity<>(ApiResponse.builder().result(result).build(), HttpStatus.OK);
      } else if (AclsNativeType.AIVEN.name().equals(clusterAclRequest.getAclNativeType())) {
        resultMap = aivenApiService.createAcls(clusterAclRequest);
        return new ResponseEntity<>(
            ApiResponse.builder().result(resultMap.get("result")).data(resultMap).build(),
            HttpStatus.OK);
      }
    } catch (Exception e) {
      resultMap.put("result", "failure " + e.getMessage());
      return new ResponseEntity<>(
          ApiResponse.builder().result("failure " + e.getMessage()).build(),
          HttpStatus.INTERNAL_SERVER_ERROR);
    }
    resultMap.put("result", "Not a valid request");
    return new ResponseEntity<>(
        ApiResponse.builder().result("Not a valid request").build(),
        HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @PostMapping(value = "/deleteAcls")
  public ResponseEntity<ApiResponse> deleteAcls(
      @RequestBody @Valid ClusterAclRequest clusterAclRequest) {
    String result;
    try {
      if (AclsNativeType.NATIVE.name().equals(clusterAclRequest.getAclNativeType())) {

        if ("Producer".equals(clusterAclRequest.getAclType()))
          result = apacheKafkaAclService.updateProducerAcl(clusterAclRequest);
        else result = apacheKafkaAclService.updateConsumerAcl(clusterAclRequest);

        return new ResponseEntity<>(ApiResponse.builder().result(result).build(), HttpStatus.OK);
      } else if (AclsNativeType.AIVEN.name().equals(clusterAclRequest.getAclNativeType())) {
        result = aivenApiService.deleteAcls(clusterAclRequest);

        return new ResponseEntity<>(ApiResponse.builder().result(result).build(), HttpStatus.OK);
      }

    } catch (Exception e) {
      log.error(e.getMessage());
      return new ResponseEntity<>(
          ApiResponse.builder().result("failure in deleting acls").build(),
          HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return new ResponseEntity<>(
        ApiResponse.builder().result("Not a valid request").build(),
        HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @PostMapping(value = "/postSchema")
  public ResponseEntity<String> postSchema(
      @RequestBody MultiValueMap<String, String> fullSchemaDetails) {
    try {
      String topicName = fullSchemaDetails.get("topicName").get(0);
      String schemaFull = fullSchemaDetails.get("fullSchema").get(0);
      String env = fullSchemaDetails.get("env").get(0);
      String protocol = fullSchemaDetails.get("protocol").get(0);

      String result = schemaService.registerSchema(topicName, schemaFull, env, protocol);
      return new ResponseEntity<>("Status:" + result, HttpStatus.OK);
    } catch (Exception e) {
      return new ResponseEntity<>("failure " + e.getMessage(), HttpStatus.OK);
    }
  }
}
