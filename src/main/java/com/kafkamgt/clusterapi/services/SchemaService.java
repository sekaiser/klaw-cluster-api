package com.kafkamgt.clusterapi.services;

import com.kafkamgt.clusterapi.models.KafkaProtocols;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;

import static com.kafkamgt.clusterapi.config.SslContextConfig.requestFactory;
import java.util.*;

@Service
@Slf4j
public class SchemaService {

    public static final String HTTPS_PREFIX = "https://";
    public static final String HTTP_PREFIX = "http://";
    @Value("${kafkawize.schemaregistry.compatibility.default:BACKWARD}")
    private
    String defaultSchemaCompatibility;

    @Value("${klaw.aiven.accesstoken:accesstoken}")
    private
    String aivenAccessToken;

    @Value("${klaw.aiven.schema.credentials:credentials}")
    private String schemaCredentials;

    public synchronized String postSchema(String topicName, String schema, String environmentVal, String protocol){
        try {
            log.info("Into post schema request TopicName:{} Env:{} Protocol:{}",topicName, environmentVal, protocol);
            if(environmentVal == null)
                return "Cannot retrieve SchemaRegistry Url";

            // set default compatibility
//            setSchemaCompatibility(environmentVal, topicName, false, protocol);
            RestTemplate restTemplate = getRestTemplate();

            String schemaRegistryUrl = null;
            if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
                schemaRegistryUrl = HTTP_PREFIX + environmentVal;
            else if(protocol.equals(KafkaProtocols.SSL.name())) {
                schemaRegistryUrl = HTTPS_PREFIX + schemaCredentials + "@" + environmentVal;
                restTemplate = new RestTemplate(requestFactory);
            }

            String uri = schemaRegistryUrl + "/subjects/" +
                    topicName + "-value/versions";


            Map<String, String> params = new HashMap<String, String>();
            params.put("schema", schema);

            HttpHeaders headers = new HttpHeaders();
            headers.set("Content-Type", "application/vnd.schemaregistry.v1+json");
            HttpEntity<Map<String, String>> request = new HttpEntity<>(params, headers);
            ResponseEntity<String> responseNew = restTemplate.postForEntity(uri, request, String.class);

            String updateTopicReqStatus = responseNew.getBody();
            log.info(responseNew.getBody());

            return updateTopicReqStatus;
        }
        catch(Exception e){
            log.error(e.getMessage());
            if(((HttpClientErrorException.Conflict) e).getStatusCode().value() == 409)
            {
                return "Schema being registered is incompatible with an earlier schema";
            }
            return "Failure in registering schema.";
        }
    }

    public TreeMap<Integer, HashMap<String, Object>> getSchema(String environmentVal, String protocol, String topicName){
        try {
            log.info("Into getSchema request {} {} {}", topicName, environmentVal, protocol);
            if (environmentVal == null)
                return null;

            List<Integer> versionsList = getSchemaVersions(environmentVal, topicName, protocol);
            String schemaCompatibility = getSchemaCompatibility(environmentVal, topicName, protocol);

            String schemaRegistryUrl = null;
            if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
                schemaRegistryUrl = HTTP_PREFIX + environmentVal;
            else if(protocol.equals(KafkaProtocols.SSL.name())) {
                schemaRegistryUrl = HTTPS_PREFIX + schemaCredentials + "@" + environmentVal;
            }

            TreeMap<Integer, HashMap<String, Object>> allSchemaObjects = new TreeMap<>();
            RestTemplate restTemplate = null;

            for (Integer schemaVersion : versionsList) {
                String uri = schemaRegistryUrl + "/subjects/" +
                        topicName + "-value/versions/" + schemaVersion;

                if(protocol.equals(KafkaProtocols.PLAINTEXT.name())) {
                    restTemplate = getRestTemplate();
                }
                else if(protocol.equals(KafkaProtocols.SSL.name())) {
                    schemaRegistryUrl = HTTPS_PREFIX + schemaCredentials + "@" + environmentVal;
                    restTemplate = new RestTemplate(requestFactory);
                }

                Map<String, String> params = new HashMap<String, String>();

                ResponseEntity<HashMap> responseNew = restTemplate.getForEntity(uri, HashMap.class, params);
                HashMap<String, Object> schemaResponse = responseNew.getBody();
                if(schemaResponse != null)
                    schemaResponse.put("compatibility", schemaCompatibility);

                log.info(Objects.requireNonNull(responseNew.getBody()).toString());
                allSchemaObjects.put(schemaVersion, schemaResponse);
            }

            return allSchemaObjects;
        }catch (Exception e)
        {
            log.error("Error from getSchema : " + e.getMessage());
            return new TreeMap<>();
        }
    }

    private List<Integer> getSchemaVersions(String environmentVal, String topicName, String protocol){
        try {
            log.info("Into getSchema versions {} {}", topicName, environmentVal);
            if (environmentVal == null)
                return null;
            String schemaRegistryUrl = null;
            RestTemplate restTemplate = getRestTemplate();

            if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
                schemaRegistryUrl = HTTP_PREFIX + environmentVal;
            else if(protocol.equals(KafkaProtocols.SSL.name())) {
                schemaRegistryUrl = HTTPS_PREFIX + schemaCredentials + "@" + environmentVal;
                restTemplate = new RestTemplate(requestFactory);
            }

            String uri = schemaRegistryUrl + "/subjects/" +
                    topicName + "-value/versions";

            Map<String, String> params = new HashMap<String, String>();

            ResponseEntity<ArrayList> responseList = restTemplate.getForEntity(uri, ArrayList.class, params);
            log.info("Schema versions " + responseList);
            return responseList.getBody();
        }catch (Exception e)
        {
            log.error("Error in getting versions " + e.getMessage());
            return new ArrayList<>();
        }
    }

    private String getSchemaCompatibility(String environmentVal, String topicName, String protocol){
        try {
            log.info("Into getSchema compatibility {} {}", topicName, environmentVal);
            if (environmentVal == null)
                return null;

            RestTemplate restTemplate = getRestTemplate();
            String schemaRegistryUrl = null;
            if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
                schemaRegistryUrl = HTTP_PREFIX + environmentVal;
            else if(protocol.equals(KafkaProtocols.SSL.name())) {
                schemaRegistryUrl = HTTPS_PREFIX + schemaCredentials + "@" + environmentVal;
                restTemplate = new RestTemplate(requestFactory);
            }

            String uri = schemaRegistryUrl + "/config/" +
                    topicName + "-value";

            Map<String, String> params = new HashMap<String, String>();

            ResponseEntity<HashMap> responseList = restTemplate.getForEntity(uri, HashMap.class, params);
            log.info("Schema compatibility " + responseList);
            return (String)responseList.getBody().get("compatibilityLevel");
        }catch (Exception e)
        {
            log.error("Error in getting schema compatibility " + e.getMessage());
            return "NOT SET";
        }
    }

    private boolean setSchemaCompatibility(String environmentVal, String topicName, boolean isForce, String protocol){
        try {
            log.info("Into setSchema compatibility {} {}", topicName, environmentVal);
            if (environmentVal == null)
                return false;
            String schemaRegistryUrl = null;
            RestTemplate restTemplate = getRestTemplate();

            if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
                schemaRegistryUrl = HTTP_PREFIX + environmentVal;
            else if(protocol.equals(KafkaProtocols.SSL.name())) {
                schemaRegistryUrl = HTTPS_PREFIX + schemaCredentials + "@" + environmentVal;
                restTemplate = new RestTemplate(requestFactory);
            }

            String uri = schemaRegistryUrl + "/config/" +
                    topicName + "-value";

            Map<String, String> params = new HashMap<>();
            if(isForce)
                params.put("compatibility", "NONE");
            else
                params.put("compatibility", defaultSchemaCompatibility);

            HttpHeaders headers = new HttpHeaders();
            headers.set("Content-Type", "application/vnd.schemaregistry.v1+json");
            HttpEntity<Map<String, String>> request = new HttpEntity<>(params, headers);
            restTemplate.put(uri, request, String.class);
            return true;
        }catch (Exception e)
        {
            log.error("Error in setting schema compatibility " + e.getMessage());
            return false;
        }
    }

    public RestTemplate getRestTemplate(){
        return new RestTemplate();
    }

//    @PostConstruct
//    public void trySchemas(){
//        String schemaRegistryUrl = "https://avnadmin:AVNS_cuCgXkKYxOVLfS0eTqZ@kafka-acls-kw-dev-sandbox.aivencloud.com:12696";
//
//        String uri = schemaRegistryUrl + "/subjects";
//        RestTemplate restTemplate = new RestTemplate(requestFactory);
//        Map<String, String> params = new HashMap<>();
//        HttpHeaders headers = new HttpHeaders();
//        headers.set("Authorization", "Bearer "+ aivenAccessToken);
//
//        HttpEntity<Map<String, String>> request = new HttpEntity<>(params, headers);
//
//        ResponseEntity<Object> responseList = restTemplate.exchange(
//                uri, HttpMethod.GET, request, new ParameterizedTypeReference<>() {});
//        System.out.println(responseList);
//    }

    protected String getSchemaRegistryStatus(String environment, String protocol){

        String schemaRegistryUrl = null;
        RestTemplate restTemplate = new RestTemplate();

        if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
            schemaRegistryUrl = HTTP_PREFIX + environment + "/subjects";
        else if(protocol.equals(KafkaProtocols.SSL.name())) {
            schemaRegistryUrl = HTTPS_PREFIX + schemaCredentials + "@" + environment + "/subjects";
            restTemplate = new RestTemplate(requestFactory);
        }

        String uri = schemaRegistryUrl ;
        Map<String, String> params = new HashMap<String, String>();

        try {
            ResponseEntity<Object> responseNew = restTemplate.getForEntity(uri, Object.class, params);
            return "ONLINE";
        } catch (RestClientException e) {
            e.printStackTrace();
            return "OFFLINE";
        }
    }
}
