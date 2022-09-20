package io.aiven.klaw.clusterapi.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.aiven.klaw.clusterapi.models.AivenAclResponse;
import io.aiven.klaw.clusterapi.models.AivenAclStruct;
import io.aiven.klaw.clusterapi.models.ResultType;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Service
@Slf4j
public class AivenApiService {

  @Value("${klaw.clusters.accesstoken:accesstoken}")
  private String clusterAccessToken;

  @Value("${klaw.clusters.listacls.api:api}")
  private String listAclsApiEndpoint;

  @Value("${klaw.clusters.addacls.api:api}")
  private String addAclsApiEndpoint;

  @Value("${klaw.clusters.deleteacls.api:api}")
  private String deleteAclsApiEndpoint;

  public Map<String, String> createAcls(MultiValueMap<String, String> permissionsMultiMap)
      throws Exception {
    Map<String, String> resultMap = new HashMap<>();
    RestTemplate restTemplate = getRestTemplate();
    String projectName = permissionsMultiMap.get("projectName").get(0);
    String serviceName = permissionsMultiMap.get("serviceName").get(0);

    Set<String> keys = permissionsMultiMap.keySet();
    LinkedHashMap<String, String> permissionsMap = new LinkedHashMap<>();
    List<String> allowedKeyList = new ArrayList<>(Arrays.asList("permission", "topic", "username"));
    for (String key : keys) {
      if (allowedKeyList.contains(key)) {
        permissionsMap.put(key, permissionsMultiMap.get(key).get(0));
      }
    }

    String uri =
        addAclsApiEndpoint.replace("projectName", projectName).replace("serviceName", serviceName);

    HttpHeaders headers = getHttpHeaders();
    HttpEntity<LinkedHashMap<String, String>> request = new HttpEntity<>(permissionsMap, headers);

    try {
      ResponseEntity<String> response = restTemplate.postForEntity(uri, request, String.class);
      AivenAclResponse aivenAclResponse =
          new ObjectMapper().readValue(response.getBody(), AivenAclResponse.class);
      Optional<AivenAclStruct> aivenAclStructOptional =
          Arrays.stream(aivenAclResponse.getAcl())
              .filter(
                  acl ->
                      acl.getUsername().equals(permissionsMultiMap.get("username").get(0))
                          && acl.getTopic().equals(permissionsMultiMap.get("topic").get(0))
                          && acl.getPermission()
                              .equals(permissionsMultiMap.get("permission").get(0)))
              .findFirst();
      aivenAclStructOptional.ifPresent(
          aivenAclStruct -> resultMap.put("aivenaclid", aivenAclStruct.getId()));

      if (response.getStatusCode().equals(HttpStatus.OK)) {
        resultMap.put("result", ResultType.SUCCESS.value);
      } else {
        resultMap.put("result", "Failure in adding acls" + response.getBody());
      }

      return resultMap;
    } catch (Exception e) {
      log.error(e.toString());
      resultMap.put("result", "Failure in adding acls" + e.getMessage());
      return resultMap;
    }
  }

  public String deleteAcls(MultiValueMap<String, String> permissionsMultiMap) throws Exception {
    RestTemplate restTemplate = getRestTemplate();

    try {
      String projectName = permissionsMultiMap.get("projectName").get(0);
      String serviceName = permissionsMultiMap.get("serviceName").get(0);
      String aclId = permissionsMultiMap.get("aivenaclid").get(0);

      String uri =
          deleteAclsApiEndpoint
              .replace("projectName", projectName)
              .replace("serviceName", serviceName)
              .replace("aclId", aclId);

      HttpHeaders headers = getHttpHeaders();
      HttpEntity<?> request = new HttpEntity<>(headers);
      restTemplate.exchange(uri, HttpMethod.DELETE, request, Object.class);
    } catch (Exception e) {
      log.error(e.toString());
      throw new Exception("Error in deleting acls " + e.getMessage());
    }

    return ResultType.SUCCESS.value;
  }

  List<LinkedHashMap<String, String>> listAcls(String projectName, String serviceName)
      throws Exception {
    RestTemplate restTemplate = getRestTemplate();

    String uri =
        listAclsApiEndpoint.replace("projectName", projectName).replace("serviceName", serviceName);

    HttpHeaders headers = getHttpHeaders();
    HttpEntity<Map<String, String>> request = new HttpEntity<>(headers);

    try {
      ResponseEntity<LinkedHashMap<String, List<LinkedHashMap<String, String>>>> responseEntity =
          restTemplate.exchange(
              uri, HttpMethod.GET, request, new ParameterizedTypeReference<>() {});

      return Objects.requireNonNull(responseEntity.getBody()).get("acl");
    } catch (RestClientException e) {
      log.error(e.toString());
      throw new Exception("Error in listing acls : " + e.getMessage());
    }
  }

  private HttpHeaders getHttpHeaders() {
    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Bearer " + clusterAccessToken);
    return headers;
  }

  private RestTemplate getRestTemplate() {
    return new RestTemplate();
  }
}
