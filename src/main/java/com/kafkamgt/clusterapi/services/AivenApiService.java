package com.kafkamgt.clusterapi.services;

import com.kafkamgt.clusterapi.utils.AdminClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.env.Environment;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.*;

@Service
@Slf4j
public class AivenApiService {

    @Autowired
    Environment env;

    @Autowired
    AdminClientUtils adminClientUtils;

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

    public String createAcls(MultiValueMap<String, String> permissionsMultiMap) throws Exception {
        RestTemplate restTemplate = adminClientUtils.getRestTemplate();
        String projectName = permissionsMultiMap.get("projectName").get(0);
        String serviceName = permissionsMultiMap.get("serviceName").get(0);

        Set<String> keys = permissionsMultiMap.keySet();
        LinkedHashMap<String, String> permissionsMap = new LinkedHashMap<>();
        for (String key : keys) {
            permissionsMap.put(key, permissionsMultiMap.get(key).get(0));
        }

        String uri = aivenAddAclsApiEndpoint.replace("projectName", projectName)
                .replace("serviceName", serviceName);

        HttpHeaders headers = getHttpHeaders();
        HttpEntity<LinkedHashMap<String, String>> request = new HttpEntity<>(permissionsMap, headers);

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(uri, request, String.class);
            if(response.getStatusCode().equals(HttpStatus.OK))
                return "success";
            else return "Failure in adding acls" + response.getBody();
        }catch (Exception e){
            throw new Exception("Error in adding acls "+ e.getMessage());
        }
    }

    public String deleteAcls(MultiValueMap<String, String> permissionsMultiMap) throws Exception {
        RestTemplate restTemplate = adminClientUtils.getRestTemplate();
        String projectName = permissionsMultiMap.get("projectName").get(0);
        String serviceName = permissionsMultiMap.get("serviceName").get(0);
        String aclId = permissionsMultiMap.get("aclId").get(0);

        String uri = aivenDeleteAclsApiEndpoint.replace("projectName", projectName)
                .replace("serviceName", serviceName)
                .replace("aclId", aclId);

        HttpHeaders headers = getHttpHeaders();
        HttpEntity<?> request = new HttpEntity<>(headers);
        try {
            restTemplate.exchange(uri, HttpMethod.DELETE, request, Object.class);
        }catch (Exception e){
            throw new Exception("Error in deleting acls "+ e.getMessage());
        }

        return "success";
    }

    ArrayList<LinkedHashMap<String, String>> listAcls(String projectName, String serviceName) throws Exception {
        RestTemplate restTemplate = adminClientUtils.getRestTemplate();

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
}
