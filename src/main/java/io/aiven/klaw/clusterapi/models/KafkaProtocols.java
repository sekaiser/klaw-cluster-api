package io.aiven.klaw.clusterapi.models;

public enum KafkaProtocols {
  PLAINTEXT,
  SSL,
  SASL,
  SASL_PLAIN,
  SASL_SSL_PLAIN_MECHANISM,
  SASL_SSL_GSSAPI_MECHANISM;
}
