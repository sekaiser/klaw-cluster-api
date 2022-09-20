package io.aiven.klaw.clusterapi.controller;

import io.aiven.klaw.clusterapi.services.KafkaConnectService;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/topics")
@Slf4j
public class KafkaConnectController {

  @Autowired KafkaConnectService kafkaConnectService;

  @RequestMapping(
      value = "/getAllConnectors/{kafkaConnectHost}/{protocol}",
      method = RequestMethod.GET,
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<List<String>> getAllConnectors(
      @PathVariable String kafkaConnectHost, @PathVariable String protocol) {
    return new ResponseEntity<>(
        kafkaConnectService.getConnectors(kafkaConnectHost, protocol), HttpStatus.OK);
  }

  @RequestMapping(
      value = "/getConnectorDetails/{connectorName}/{kafkaConnectHost}/{protocol}",
      method = RequestMethod.GET,
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<LinkedHashMap<String, Object>> getConnectorDetails(
      @PathVariable String connectorName,
      @PathVariable String kafkaConnectHost,
      @PathVariable String protocol) {
    return new ResponseEntity<>(
        kafkaConnectService.getConnectorDetails(connectorName, kafkaConnectHost, protocol),
        HttpStatus.OK);
  }

  @PostMapping(value = "/postConnector")
  public ResponseEntity<Map<String, String>> postConnector(
      @RequestBody MultiValueMap<String, String> fullConnectorConfig) {
    String env = fullConnectorConfig.get("env").get(0);
    String protocol = fullConnectorConfig.get("protocol").get(0);
    String connectorConfig = fullConnectorConfig.get("connectorConfig").get(0);

    Map<String, String> result =
        kafkaConnectService.postNewConnector(env, protocol, connectorConfig);
    return new ResponseEntity<>(result, HttpStatus.OK);
  }

  @PostMapping(value = "/updateConnector")
  public ResponseEntity<Map<String, String>> updateConnector(
      @RequestBody MultiValueMap<String, String> fullConnectorConfig) {
    String env = fullConnectorConfig.get("env").get(0);
    String protocol = fullConnectorConfig.get("protocol").get(0);
    String connectorName = fullConnectorConfig.get("connectorName").get(0);
    String connectorConfig = fullConnectorConfig.get("connectorConfig").get(0);

    Map<String, String> result =
        kafkaConnectService.updateConnector(env, protocol, connectorName, connectorConfig);
    return new ResponseEntity<>(result, HttpStatus.OK);
  }

  @PostMapping(value = "/deleteConnector")
  public ResponseEntity<Map<String, String>> deleteConnector(
      @RequestBody MultiValueMap<String, String> connectorConfig) {
    String env = connectorConfig.get("env").get(0);
    String protocol = connectorConfig.get("protocol").get(0);
    String connectorName = connectorConfig.get("connectorName").get(0);

    Map<String, String> result = kafkaConnectService.deleteConnector(env, protocol, connectorName);
    return new ResponseEntity<>(result, HttpStatus.OK);
  }
}
