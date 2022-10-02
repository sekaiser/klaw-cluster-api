package io.aiven.klaw.clusterapi.utils;

import com.google.common.base.Strings;
import io.aiven.klaw.clusterapi.config.SslContextConfig;
import io.aiven.klaw.clusterapi.models.KafkaClustersType;
import io.aiven.klaw.clusterapi.models.KafkaProtocols;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
@Slf4j
public class ClusterApiUtils {

  public static final String HTTPS_PREFIX = "https://";
  public static final String HTTP_PREFIX = "http://";

  final Environment env;

  @Value("${klaw.request.timeout.ms:15000}")
  private String requestTimeOutMs;

  @Value("${klaw.retries.config:25}")
  private String retriesConfig;

  @Value("${klaw.retry.backoff.ms:15000}")
  private String retryBackOffMsConfig;

  @Value("${klaw.aiven.kafkaconnect.credentials:credentials}")
  private String connectCredentials;

  @Value("${klaw.aiven.karapace.credentials}")
  private String schemaRegistryCredentials;

  private final HashMap<String, AdminClient> adminClientsMap = new HashMap<>();
  ;

  private static MessageDigest messageDigest;

  static {
    try {
      messageDigest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  public ClusterApiUtils(Environment env) {
    this.env = env;
  }

  public RestTemplate getRestTemplate() {
    return new RestTemplate();
  }

  //    public void removeSSLElementFromAdminClientMap(String protocol, String clusterName){
  //        log.info("Into removeSSLElementFromAdminClientMap");
  //        String adminClientKeyReq = protocol + clusterName;
  //        List<String> sslKeys = adminClientsMap.keySet().stream()
  //                .filter(adminClientKey -> adminClientKey.equals(adminClientKeyReq))
  //                .collect(Collectors.toList());
  //        sslKeys.forEach(adminClientsMap::remove);
  //    }

  private String getHash(String envHost) {
    return new String(Base64.getEncoder().encode(messageDigest.digest(envHost.getBytes())));
  }

  public AdminClient getAdminClient(String envHost, String protocol, String clusterNameTenantId)
      throws Exception {
    log.info(
        "Host : {} Protocol {} clusterNameTenantId {}", envHost, protocol, clusterNameTenantId);

    AdminClient adminClient = null;
    String adminClientKey = protocol + clusterNameTenantId + getHash(envHost);

    try {
      switch (protocol) {
        case "PLAINTEXT":
          if (!adminClientsMap.containsKey(adminClientKey)) {
            adminClient = AdminClient.create(getPlainProperties(envHost));
          } else {
            adminClient = adminClientsMap.get(adminClientKey);
          }
          break;

        case "SSL":
          if (!adminClientsMap.containsKey(adminClientKey)) {
            adminClient = AdminClient.create(getSslProperties(envHost, clusterNameTenantId));
          } else {
            adminClient = adminClientsMap.get(adminClientKey);
          }
          break;

        case "SASL_PLAIN":
          if (!adminClientsMap.containsKey(adminClientKey)) {
            adminClient = AdminClient.create(getSaslPlainProperties(envHost, clusterNameTenantId));
          } else {
            adminClient = adminClientsMap.get(adminClientKey);
          }
          break;

        case "SASL_SSL-PLAINMECHANISM":
          if (!adminClientsMap.containsKey(adminClientKey)) {
            adminClient =
                AdminClient.create(
                    getSaslSsl_PlainMechanismProperties(envHost, clusterNameTenantId));
          } else {
            adminClient = adminClientsMap.get(adminClientKey);
          }
          break;

        case "SASL_SSL-SCRAMMECHANISM":
          if (!adminClientsMap.containsKey(adminClientKey)) {
            adminClient =
                AdminClient.create(
                    getSaslSsl_ScramMechanismProperties(envHost, clusterNameTenantId));
          } else {
            adminClient = adminClientsMap.get(adminClientKey);
          }
          break;

        case "SASL_SSL-GSSAPIMECHANISM":
          if (!adminClientsMap.containsKey(adminClientKey)) {
            adminClient =
                AdminClient.create(
                    getSaslSsl_GSSAPIMechanismProperties(envHost, clusterNameTenantId));
          } else {
            adminClient = adminClientsMap.get(adminClientKey);
          }
          break;
      }
    } catch (Exception exception) {
      log.error("Unable to create Admin client " + exception.getMessage() + exception.getCause());
      exception.printStackTrace();
      throw new Exception("Cannot connect to cluster. Please contact Administrator.");
    }

    if (adminClient == null) {
      log.error("Cannot create Admin Client {} {}", envHost, protocol);
      throw new Exception("Cannot connect to cluster. Please contact Administrator.");
    }

    try {
      adminClient.listTopics().names().get();
      if (!adminClientsMap.containsKey(adminClientKey)) {
        adminClientsMap.put(adminClientKey, adminClient);
      }
      return adminClient;
    } catch (Exception e) {
      adminClientsMap.remove(adminClientKey);
      adminClient.close();
      log.error("Cannot create Admin Client {} {} {}", envHost, protocol, clusterNameTenantId);
      throw new Exception("Cannot connect to cluster. Please contact Administrator.");
    }
  }

  public Properties getPlainProperties(String environment) {
    Properties props = new Properties();

    props.put("bootstrap.servers", environment);
    setOtherConfig(props);

    return props;
  }

  public Properties getSslProperties(String environment, String clusterName) {
    Properties props = getSslConfig(clusterName);

    props.put("bootstrap.servers", environment);
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "klawclientssl");
    setOtherConfig(props);

    return props;
  }

  public Properties getSaslPlainProperties(String environment, String clusterName) {
    Properties props = new Properties();

    props.put("bootstrap.servers", environment);

    try {
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
      props.put(AdminClientConfig.CLIENT_ID_CONFIG, "klawclientsaslplain");
      setOtherConfig(props);

      if (!Strings.isNullOrEmpty(env.getProperty("kafkasasl.saslmechanism.plain"))) {
        props.put(SaslConfigs.SASL_MECHANISM, env.getProperty("kafkasasl.saslmechanism.plain"));
      }

      if (!Strings.isNullOrEmpty(
          env.getProperty(clusterName.toLowerCase() + ".kafkasasl.jaasconfig.plain"))) {
        props.put(
            SaslConfigs.SASL_JAAS_CONFIG,
            env.getProperty(clusterName.toLowerCase() + ".kafkasasl.jaasconfig.plain"));
      }

    } catch (Exception exception) {
      log.error("Error : Cannot set SASL PLAIN Config properties.");
    }

    return props;
  }

  public Properties getSaslSsl_PlainMechanismProperties(String environment, String clusterName) {
    Properties props = getSslConfig(clusterName);

    try {
      props.put("bootstrap.servers", environment);
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
      props.put(AdminClientConfig.CLIENT_ID_CONFIG, "klawclientsaslsslplain");
      setOtherConfig(props);

      if (!Strings.isNullOrEmpty(env.getProperty("kafkasasl.saslmechanism.plain"))) {
        props.put(SaslConfigs.SASL_MECHANISM, env.getProperty("kafkasasl.saslmechanism.plain"));
      }
      if (!Strings.isNullOrEmpty(
          env.getProperty(clusterName.toLowerCase() + ".kafkasasl.jaasconfig.plain"))) {
        props.put(
            SaslConfigs.SASL_JAAS_CONFIG,
            env.getProperty(clusterName.toLowerCase() + ".kafkasasl.jaasconfig.plain"));
      }
    } catch (Exception exception) {
      log.error("Error : Cannot set SASL SSL PLAIN Config properties.");
    }

    return props;
  }

  public Properties getSaslSsl_ScramMechanismProperties(String environment, String clusterName) {
    Properties props = getSslConfig(clusterName);

    try {
      props.put("bootstrap.servers", environment);
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
      props.put(AdminClientConfig.CLIENT_ID_CONFIG, "klawclientsaslsslscram");
      setOtherConfig(props);

      if (!Strings.isNullOrEmpty(env.getProperty("kafkasasl.saslmechanism.scram"))) {
        props.put(SaslConfigs.SASL_MECHANISM, env.getProperty("kafkasasl.saslmechanism.scram"));
      }
      if (!Strings.isNullOrEmpty(
          env.getProperty(clusterName.toLowerCase() + ".kafkasasl.jaasconfig.scram"))) {
        props.put(
            SaslConfigs.SASL_JAAS_CONFIG,
            env.getProperty(clusterName.toLowerCase() + ".kafkasasl.jaasconfig.scram"));
      }
    } catch (Exception exception) {
      log.error("Error : Cannot set SASL SSL SCRAM Config properties.");
    }

    return props;
  }

  public Properties getSaslSsl_GSSAPIMechanismProperties(String environment, String clusterName) {
    Properties props = getSslConfig(clusterName);

    try {
      props.put("bootstrap.servers", environment);
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
      props.put(AdminClientConfig.CLIENT_ID_CONFIG, "klawclientsaslsslgssapi");
      setOtherConfig(props);

      if (!Strings.isNullOrEmpty(env.getProperty("kafkasasl.saslmechanism.gssapi"))) {
        props.put(SaslConfigs.SASL_MECHANISM, env.getProperty("kafkasasl.saslmechanism.gssapi"));
      }

      if (!Strings.isNullOrEmpty(
          env.getProperty(clusterName.toLowerCase() + ".kafkasasl.jaasconfig.gssapi"))) {
        props.put(
            SaslConfigs.SASL_JAAS_CONFIG,
            env.getProperty(clusterName.toLowerCase() + ".kafkasasl.jaasconfig.gssapi"));
      }

      if (!Strings.isNullOrEmpty(
          env.getProperty(
              clusterName.toLowerCase() + ".kafkasasl.saslmechanism.gssapi.servicename"))) {
        props.put(
            SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
            env.getProperty(
                clusterName.toLowerCase() + ".kafkasasl.saslmechanism.gssapi.servicename"));
      }
    } catch (Exception exception) {
      log.error("Error : Cannot set SASL SSL GSSAPI Config properties.");
    }

    return props;
  }

  public Properties getSslConfig(String clusterName) {
    //        if(kwInstallationType.equals("saas"))
    clusterName = "klawssl";

    Properties props = new Properties();

    try {
      if (!Strings.isNullOrEmpty(
          env.getProperty(clusterName.toLowerCase() + ".kafkassl.keystore.location"))) {
        props.put(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            env.getProperty(clusterName.toLowerCase() + ".kafkassl.keystore.location"));
      }
      if (!Strings.isNullOrEmpty(
          env.getProperty(clusterName.toLowerCase() + ".kafkassl.keystore.pwd"))) {
        props.put(
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            env.getProperty(clusterName.toLowerCase() + ".kafkassl.keystore.pwd"));
      }
      if (!Strings.isNullOrEmpty(
          env.getProperty(clusterName.toLowerCase() + ".kafkassl.key.pwd"))) {
        props.put(
            SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            env.getProperty(clusterName.toLowerCase() + ".kafkassl.key.pwd"));
      }
      if (!Strings.isNullOrEmpty(
          env.getProperty(clusterName.toLowerCase() + ".kafkassl.keystore.type"))) {
        props.put(
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
            env.getProperty(clusterName.toLowerCase() + ".kafkassl.keystore.type"));
      } else {
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
      }
      if (!Strings.isNullOrEmpty(
          env.getProperty(clusterName.toLowerCase() + ".kafkassl.truststore.type"))) {
        props.put(
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
            env.getProperty(clusterName.toLowerCase() + ".kafkassl.truststore.type"));
      } else {
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
      }

      if (!Strings.isNullOrEmpty(
          env.getProperty(clusterName.toLowerCase() + ".kafkassl.truststore.location"))) {
        props.put(
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            env.getProperty(clusterName.toLowerCase() + ".kafkassl.truststore.location"));
      }

      if (!Strings.isNullOrEmpty(
          env.getProperty(clusterName.toLowerCase() + ".kafkassl.truststore.pwd"))) {
        props.put(
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            env.getProperty(clusterName.toLowerCase() + ".kafkassl.truststore.pwd"));
      }

      props.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1");
      props.put("ssl.endpoint.identification.algorithm", "");
    } catch (Exception exception) {
      log.error("Error : Cannot set SSL Config properties.");
    }

    return props;
  }

  private void setOtherConfig(Properties props) {
    props.put(AdminClientConfig.RETRIES_CONFIG, retriesConfig);
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeOutMs);
    props.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, retryBackOffMsConfig);
  }

  public Pair<String, RestTemplate> getRequestDetails(
      String suffixUrl, String protocol, KafkaClustersType kafkaClustersType) {
    RestTemplate restTemplate;
    String connectorsUrl = "";

    if (KafkaProtocols.PLAINTEXT.name().equals(protocol)) {
      connectorsUrl = HTTP_PREFIX + suffixUrl;
      restTemplate = new RestTemplate();
    } else if (KafkaProtocols.SSL.name().equals(protocol)) {
      if (KafkaClustersType.KAFKA_CONNECT.equals(kafkaClustersType))
        connectorsUrl = HTTPS_PREFIX + connectCredentials + "@" + suffixUrl;
      else if (KafkaClustersType.SCHEMA_REGISTRY.equals(kafkaClustersType))
        connectorsUrl = HTTPS_PREFIX + schemaRegistryCredentials + "@" + suffixUrl;
      restTemplate = new RestTemplate(SslContextConfig.requestFactory);
    } else {
      restTemplate = new RestTemplate();
    }

    return Pair.of(connectorsUrl, restTemplate);
  }
}
