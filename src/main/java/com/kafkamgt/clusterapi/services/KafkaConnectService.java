package com.kafkamgt.clusterapi.services;

import com.kafkamgt.clusterapi.models.KafkaProtocols;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.kafkamgt.clusterapi.config.SslContextConfig.requestFactory;

@Service
@Slf4j
public class KafkaConnectService {

    public static final String HTTPS_PREFIX = "https://";
    public static final String HTTP_PREFIX = "http://";

    @Value("${klaw.aiven.kafkaconnect.credentials:credentials}")
    private String connectCredentials;

    public RestTemplate getRestTemplate(){
        return new RestTemplate();
    }

    public HashMap<String, String> deleteConnector(String environmentVal, String protocol, String connectorName) {
        log.info("Into deleteConnector {} {} {}", environmentVal, connectorName, protocol);
        HashMap<String, String> result = new HashMap<>();
        RestTemplate restTemplate = getRestTemplate();

        if (environmentVal == null)
            return null;

        String connectorsUrl = null;
        if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
            connectorsUrl = HTTP_PREFIX + environmentVal + "/connectors/" + connectorName;
        else if(protocol.equals(KafkaProtocols.SSL.name())) {
            connectorsUrl = HTTPS_PREFIX + connectCredentials + "@" + environmentVal + "/connectors/" + connectorName;
            restTemplate = new RestTemplate(requestFactory);
        }

        try {
            restTemplate.delete(connectorsUrl, String.class);
        } catch (RestClientException e) {
            log.error("Error in deleting connector " + e.toString());
            result.put("result", "error");
            result.put("errorText", e.toString().replaceAll("\"",""));
            return result;
        }
        result.put("result","success");
        return result;
    }

    public HashMap<String, String> updateConnector(String environmentVal, String protocol, String connectorName, String connectorConfig){
        log.info("Into updateConnector {} {} {} {}",  environmentVal, connectorName, connectorConfig, protocol);
        HashMap<String, String> result = new HashMap<>();

        if (environmentVal == null)
            return null;

        RestTemplate restTemplate = getRestTemplate();
        String connectorsUrl = null;
        if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
            connectorsUrl = HTTP_PREFIX + environmentVal + "/connectors/" + connectorName + "/config";
        else if(protocol.equals(KafkaProtocols.SSL.name())) {
            connectorsUrl = HTTPS_PREFIX + connectCredentials + "@" + environmentVal + "/connectors/" + connectorName + "/config";
            restTemplate = new RestTemplate(requestFactory);
        }

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<String> request = new HttpEntity<>(connectorConfig, headers);

        try {
            restTemplate.put(connectorsUrl, request, String.class);
        } catch (RestClientException e) {
            log.error("Error in updating connector " + e.toString());
            result.put("result", "error");
            result.put("errorText", e.toString().replaceAll("\"",""));
            return result;
        }
        result.put("result","success");

        return result;
    }

    public HashMap<String, String> postNewConnector(String environmentVal, String protocol, String connectorConfig){
        log.info("Into postNewConnector {} {} {}",  environmentVal, connectorConfig, protocol);
        HashMap<String, String> result = new HashMap<>();
        if (environmentVal == null)
            return null;

        RestTemplate restTemplate = getRestTemplate();
        String connectorsUrl = null;
        if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
            connectorsUrl = HTTP_PREFIX + environmentVal + "/connectors";
        else if(protocol.equals(KafkaProtocols.SSL.name())) {
            connectorsUrl = HTTPS_PREFIX + connectCredentials + "@" + environmentVal + "/connectors";
            restTemplate = new RestTemplate(requestFactory);
        }

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<String> request = new HttpEntity<>(connectorConfig, headers);
        ResponseEntity<String> responseNew ;
        try {
            responseNew = restTemplate.postForEntity(connectorsUrl, request, String.class);
        } catch (RestClientException e) {
            log.error("Error in registering new connector " + e.toString());
            result.put("result", "error");
            result.put("errorText", e.toString().replaceAll("\"",""));
            return result;
        }
        if(responseNew.getStatusCodeValue() == 201) {
            result.put("result","success");
        }
        else {
            result.put("result","failure");
        }
        return result;
    }

    public ArrayList<String> getConnectors(String environmentVal, String protocol){
        try {
            log.info("Into getConnectors {} {}",  environmentVal, protocol);
            if (environmentVal == null)
                return null;

            RestTemplate restTemplate = getRestTemplate();
            String connectorsUrl = null;

            if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
                connectorsUrl = HTTP_PREFIX + environmentVal;
            else if(protocol.equals(KafkaProtocols.SSL.name())) {
                connectorsUrl = HTTPS_PREFIX + connectCredentials + "@" + environmentVal;
                restTemplate = new RestTemplate(requestFactory);
            }

            String uri = connectorsUrl + "/connectors";
            Map<String, String> params = new HashMap<>();
            ResponseEntity<ArrayList> responseList = restTemplate.getForEntity(uri, ArrayList.class, params);

            log.info("connectors list " + responseList);
            return  responseList.getBody();
        }catch (Exception e)
        {
            log.error("Error in getting connectors " + e.getMessage());
            return new ArrayList<>();
        }
    }

    public LinkedHashMap<String, Object> getConnectorDetails(String connector, String environmentVal, String protocol){
        try {
            log.info("Into getConnectorDetails {} {}",  environmentVal, protocol);
            if (environmentVal == null)
                return null;

            RestTemplate restTemplate = getRestTemplate();
            String connectorsUrl = null;

            if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
                connectorsUrl = HTTP_PREFIX + environmentVal + "/connectors" + "/" + connector;
            else if(protocol.equals(KafkaProtocols.SSL.name())) {
                connectorsUrl = HTTPS_PREFIX + connectCredentials + "@" + environmentVal + "/connectors" + "/" + connector;
                restTemplate = new RestTemplate(requestFactory);
            }

            Map<String, String> params = new HashMap<>();

            ResponseEntity<LinkedHashMap> responseList = restTemplate.getForEntity(connectorsUrl, LinkedHashMap.class, params);
            log.info("connectors list " + responseList);

            return responseList.getBody();
        }catch (Exception e)
        {
            log.error("Error in getting connector detail " + e.getMessage());
            return new LinkedHashMap<>();
        }
    }

    protected String getKafkaConnectStatus(String environment, String protocol) {
        String connectorsUrl = null;
        RestTemplate restTemplate = new RestTemplate();

        if(protocol.equals(KafkaProtocols.PLAINTEXT.name()))
            connectorsUrl = HTTP_PREFIX + environment + "/connectors";
        else if(protocol.equals(KafkaProtocols.SSL.name())) {
            connectorsUrl = HTTPS_PREFIX + connectCredentials + "@" + environment + "/connectors";
            restTemplate = new RestTemplate(requestFactory);
        }

        String uri = connectorsUrl ;
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
