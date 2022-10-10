package io.aiven.klaw.clusterapi.services;

import io.aiven.klaw.clusterapi.models.ClusterAclRequest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AivenApiServiceTest {

  AivenApiService aivenApiService;

  @Before
  public void setUp() {
    aivenApiService = new AivenApiService();
  }

  @Ignore
  @Test
  public void getAclsListTest() throws Exception {
    // TODO when, asserts
    aivenApiService.listAcls("dev-sandbox", "kafka-acls-kw");
  }

  @Ignore
  @Test
  public void createAclsTest() throws Exception {
    // TODO when, asserts
    ClusterAclRequest clusterAclRequest =
        ClusterAclRequest.builder()
            .permission("read")
            .topicName("testtopic")
            .username("avnadmin")
            .projectName("testproject")
            .serviceName("serviceName")
            .build();

    aivenApiService.createAcls(clusterAclRequest);
  }

  @Ignore
  @Test
  public void deleteAclsTest() throws Exception {
    // TODO when, asserts
    ClusterAclRequest clusterAclRequest =
        ClusterAclRequest.builder()
            .aivenAclKey("4322342")
            .projectName("testproject")
            .serviceName("serviceName")
            .build();

    aivenApiService.deleteAcls(clusterAclRequest);
  }
}
