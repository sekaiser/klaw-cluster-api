package io.aiven.klaw.clusterapi.models;

public enum ResultType {
  SUCCESS("success"),
  ERROR("error"),
  FAILURE("failure");

  public final String value;

  private ResultType(String value) {
    this.value = value;
  }
}
