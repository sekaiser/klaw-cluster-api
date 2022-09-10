package com.kafkamgt.clusterapi.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkamgt.clusterapi.models.AivenAclResponse;
import com.kafkamgt.clusterapi.models.AivenAclStruct;
import com.kafkamgt.clusterapi.utils.AdminClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.env.Environment;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.*;

@Service
@Slf4j
public class AivenApiService {

    @Value("${klaw.aiven.accesstoken:accesstoken}")
    private
    String aivenAccessToken;

    @Value("${klaw.aiven.listacls.api:api}")
    private
    String aivenListAclsApiEndpoint;

    @Value("${klaw.aiven.addacls.api:api}")
    private
    String aivenAddAclsApiEndpoint;

    @Value("${klaw.aiven.deleteacls.api:api}")
    private
    String aivenDeleteAclsApiEndpoint;

    @PostConstruct
    private void verifyAcls(){
        try {
            MultiValueMap<String, String> linkedHashMap = new LinkedMultiValueMap<>();
            linkedHashMap.add("permission","read");
            linkedHashMap.add("topic","testtopic");
            linkedHashMap.add("username","avnadmin6");
            linkedHashMap.add("projectName", "dev-sandbox");
            linkedHashMap.add("serviceName","kafka-acls-kw");
            createAcls(linkedHashMap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HashMap<String, String> createAcls(MultiValueMap<String, String> permissionsMultiMap) throws Exception {
        HashMap<String, String> resultMap = new HashMap<>();
        RestTemplate restTemplate = getRestTemplate();
        String projectName = permissionsMultiMap.get("projectName").get(0);
        String serviceName = permissionsMultiMap.get("serviceName").get(0);

        Set<String> keys = permissionsMultiMap.keySet();
        LinkedHashMap<String, String> permissionsMap = new LinkedHashMap<>();
        ArrayList<String> allowedKeyList = new ArrayList<>(Arrays.asList("permission", "topic", "username"));
        for (String key : keys) {
            if(allowedKeyList.contains(key)) {
                permissionsMap.put(key, permissionsMultiMap.get(key).get(0));
            }
        }

        String uri = aivenAddAclsApiEndpoint.replace("projectName", projectName)
                .replace("serviceName", serviceName);

        HttpHeaders headers = getHttpHeaders();
        HttpEntity<LinkedHashMap<String, String>> request = new HttpEntity<>(permissionsMap, headers);

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(uri, request, String.class);
            AivenAclResponse aivenAclResponse = new ObjectMapper().readValue(response.getBody(), AivenAclResponse.class);
            Optional<AivenAclStruct> aivenAclStructOptional =  Arrays.stream(aivenAclResponse.getAcl())
                    .filter(acl -> acl.getUsername().equals(permissionsMultiMap.get("username").get(0))
                    && acl.getTopic().equals(permissionsMultiMap.get("topic").get(0))
                    && acl.getPermission().equals(permissionsMultiMap.get("permission").get(0)))
                    .findFirst();
            aivenAclStructOptional.ifPresent(aivenAclStruct -> resultMap.put("aivenaclid", aivenAclStruct.getId()));

            if(response.getStatusCode().equals(HttpStatus.OK)){
                resultMap.put("result", "success");
            }
            else {
                resultMap.put("result", "Failure in adding acls" + response.getBody());
            }

            return resultMap;
        }catch (Exception e){
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

            String uri = aivenDeleteAclsApiEndpoint.replace("projectName", projectName)
                    .replace("serviceName", serviceName)
                    .replace("aclId", aclId);

            HttpHeaders headers = getHttpHeaders();
            HttpEntity<?> request = new HttpEntity<>(headers);
            restTemplate.exchange(uri, HttpMethod.DELETE, request, Object.class);
        }catch (Exception e){
            throw new Exception("Error in deleting acls "+ e.getMessage());
        }

        return "success";
    }

    ArrayList<LinkedHashMap<String, String>> listAcls(String projectName, String serviceName) throws Exception {
        RestTemplate restTemplate = getRestTemplate();

        String uri = aivenListAclsApiEndpoint.replace("projectName", projectName)
                .replace("serviceName", serviceName);

        HttpHeaders headers = getHttpHeaders();
        HttpEntity<Map<String, String>> request = new HttpEntity<>(headers);

        try {
            ResponseEntity<LinkedHashMap<String, ArrayList<LinkedHashMap<String, String>>>> responseEntity = restTemplate.exchange(
                    uri, HttpMethod.GET, request, new ParameterizedTypeReference<>() {});

            return Objects.requireNonNull(responseEntity.getBody()).get("acl");
        } catch (RestClientException e) {
            throw new Exception("Error in listing acls : "+e.getMessage());
        }
    }

    private HttpHeaders getHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer "+ aivenAccessToken);
        return headers;
    }

    private RestTemplate getRestTemplate(){
        return new RestTemplate();
    }
}
