package io.aiven.klaw.clusterapi.services;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

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
    MultiValueMap<String, String> linkedHashMap = new LinkedMultiValueMap<>();
    linkedHashMap.add("permission", "read");
    linkedHashMap.add("topic", "testtopic");
    linkedHashMap.add("username", "avnadmin");
    linkedHashMap.add("projectName", "testproject");
    linkedHashMap.add("serviceName", "serviceName");

    aivenApiService.createAcls(linkedHashMap);
  }

  @Ignore
  @Test
  public void deleteAclsTest() throws Exception {
    // TODO when, asserts
    MultiValueMap<String, String> linkedHashMap = new LinkedMultiValueMap<>();
    linkedHashMap.add("aclId", "12345abcdef");
    linkedHashMap.add("projectName", "testproject");
    linkedHashMap.add("serviceName", "serviceName");

    aivenApiService.deleteAcls(linkedHashMap);
  }
}
