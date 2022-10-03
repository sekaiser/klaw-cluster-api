package io.aiven.klaw.clusterapi.models;

public enum AclOperationType {
  CREATE("Create"),
  DELETE("Delete");

  public final String value;

  AclOperationType(String value) {
    this.value = value;
  }
}
