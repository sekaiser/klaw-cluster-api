package io.aiven.klaw.clusterapi.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.aiven.klaw.clusterapi.UtilMethods;
import io.aiven.klaw.clusterapi.models.AclType;
import io.aiven.klaw.clusterapi.models.ApiResponse;
import io.aiven.klaw.clusterapi.models.ApiResultStatus;
import io.aiven.klaw.clusterapi.models.ClusterAclRequest;
import io.aiven.klaw.clusterapi.models.ClusterSchemaRequest;
import io.aiven.klaw.clusterapi.models.ClusterStatus;
import io.aiven.klaw.clusterapi.models.ClusterTopicRequest;
import io.aiven.klaw.clusterapi.services.ApacheKafkaAclService;
import io.aiven.klaw.clusterapi.services.ApacheKafkaTopicService;
import io.aiven.klaw.clusterapi.services.SchemaService;
import io.aiven.klaw.clusterapi.services.UtilComponentsService;
import java.util.Set;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@RunWith(SpringJUnit4ClassRunner.class)
public class ClusterApiControllerTest {

  @MockBean private UtilComponentsService utilComponentsService;

  @MockBean private ApacheKafkaAclService apacheKafkaAclService;
  @MockBean private ApacheKafkaTopicService apacheKafkaTopicService;
  @MockBean private SchemaService schemaService;

  private MockMvc mvc;

  private ClusterApiController clusterApiController;

  private UtilMethods utilMethods;

  @Before
  public void setUp() throws Exception {
    clusterApiController = new ClusterApiController();
    mvc = MockMvcBuilders.standaloneSetup(clusterApiController).dispatchOptions(true).build();
    utilMethods = new UtilMethods();
    ReflectionTestUtils.setField(
        clusterApiController, "manageKafkaComponents", utilComponentsService);
  }

  @Test
  public void getApiStatus() throws Exception {
    String res =
        mvc.perform(
                MockMvcRequestBuilders.get("/topics/getApiStatus")
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    assertEquals("ONLINE", res);
  }

  @Test
  @Ignore
  public void getStatus() throws Exception {
    String env = "DEV";
    when(utilComponentsService.getStatus(env, "PLAINTEXT", "", ""))
        .thenReturn(ClusterStatus.ONLINE);

    String res =
        mvc.perform(
                MockMvcRequestBuilders.get("/topics/getStatus/" + env + "/PLAINTEXT")
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    assertEquals("ONLINE", res);
  }

  @Test
  @Ignore
  public void getTopics() throws Exception {
    String env = "DEV";
    when(apacheKafkaTopicService.loadTopics(env, "PLAINTEXT", ""))
        .thenReturn(utilMethods.getTopics());

    String res =
        mvc.perform(
                MockMvcRequestBuilders.get("/topics/getTopics/" + env + "/PLAINTEXT")
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    Set response = new ObjectMapper().readValue(res, Set.class);
    assertEquals(1, response.size());
  }

  @Test
  @Ignore
  public void getAcls() throws Exception {
    String env = "DEV";
    when(apacheKafkaAclService.loadAcls(env, "PLAINTEXT", "")).thenReturn(utilMethods.getAcls());

    String res =
        mvc.perform(
                MockMvcRequestBuilders.get("/topics/getAcls/" + env + "/PLAINTEXT")
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    Set response = new ObjectMapper().readValue(res, Set.class);
    assertEquals(2, response.size());
  }

  @Test
  @Ignore
  public void createTopics() throws Exception {
    ClusterTopicRequest topicRequest = utilMethods.getTopicRequest();
    String jsonReq = new ObjectMapper().writer().writeValueAsString(topicRequest);
    ApiResponse apiResponse = ApiResponse.builder().result(ApiResultStatus.SUCCESS.value).build();

    when(apacheKafkaTopicService.createTopic(topicRequest)).thenReturn(apiResponse);

    String response =
        mvc.perform(
                MockMvcRequestBuilders.post("/topics/createTopics")
                    .content(jsonReq)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    // assertEquals(ApiResultStatus.SUCCESS.value, response);
    assertThat(response, CoreMatchers.containsString(ApiResultStatus.SUCCESS.value));
  }

  @Test
  @Ignore
  public void createAclsProducer() throws Exception {
    ClusterAclRequest clusterAclRequest = utilMethods.getAclRequest(AclType.PRODUCER.value);
    String jsonReq = new ObjectMapper().writer().writeValueAsString(clusterAclRequest);

    when(apacheKafkaAclService.updateProducerAcl(clusterAclRequest))
        .thenReturn(ApiResultStatus.SUCCESS.value);

    String response =
        mvc.perform(
                MockMvcRequestBuilders.post("/topics/createAcls")
                    .content(jsonReq)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    // assertEquals(ApiResultStatus.SUCCESS.value, response);
    assertThat(response, CoreMatchers.containsString(ApiResultStatus.SUCCESS.value));
  }

  @Test
  @Ignore
  public void createAclsConsumer() throws Exception {
    ClusterAclRequest clusterAclRequest = utilMethods.getAclRequest(AclType.CONSUMER.value);
    String jsonReq = new ObjectMapper().writer().writeValueAsString(clusterAclRequest);
    //    MultiValueMap<String, String> topicRequest =
    // utilMethods.getMappedValuesAcls(AclType.CONSUMER.value);
    //
    //    String jsonReq = new ObjectMapper().writeValueAsString(topicRequest);

    when(apacheKafkaAclService.updateConsumerAcl(clusterAclRequest))
        //            topicRequest.get("topicName").get(0),
        //            topicRequest.get("env").get(0),
        //            topicRequest.get("protocol").get(0),
        //            topicRequest.get("clusterName").get(0),
        //            topicRequest.get("acl_ip").get(0),
        //            topicRequest.get("acl_ssl").get(0),
        //            topicRequest.get("consumerGroup").get(0),
        //            "Create",
        //            "false",
        //            AclIPPrincipleType.PRINCIPLE.name(),
        //            AclsNativeType.NATIVE.name()))
        .thenReturn("success1");

    String response =
        mvc.perform(
                MockMvcRequestBuilders.post("/topics/createAcls")
                    .content(jsonReq)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    // assertEquals(ApiResultStatus.SUCCESS.value, response);
    assertThat(response, CoreMatchers.containsString(ApiResultStatus.SUCCESS.value));
  }

  @Test
  @Ignore
  public void createAclsConsumerFail() throws Exception {
    ClusterAclRequest clusterAclRequest = utilMethods.getAclRequest(AclType.CONSUMER.value);
    String jsonReq = new ObjectMapper().writer().writeValueAsString(clusterAclRequest);
    //    MultiValueMap<String, String> topicRequest =
    // utilMethods.getMappedValuesAcls(AclType.CONSUMER.value);
    //    String jsonReq = new ObjectMapper().writer().writeValueAsString(topicRequest);

    when(apacheKafkaAclService.updateConsumerAcl(clusterAclRequest))
        //            topicRequest.get("topicName").get(0),
        //            topicRequest.get("env").get(0),
        //            topicRequest.get("protocol").get(0),
        //            topicRequest.get("clusterName").get(0),
        //            topicRequest.get("acl_ip").get(0),
        //            topicRequest.get("acl_ssl").get(0),
        //            topicRequest.get("consumerGroup").get(0),
        //            "Create",
        //            "false",
        //            AclIPPrincipleType.PRINCIPAL.name(),
        //            AclsNativeType.NATIVE.name()))
        .thenThrow(new RuntimeException("Error creating acls"));

    String response =
        mvc.perform(
                MockMvcRequestBuilders.post("/topics/createAcls")
                    .content(jsonReq)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    assertThat(response, CoreMatchers.containsString(ApiResultStatus.SUCCESS.value));
  }

  @Test
  public void postSchema() throws Exception {
    ClusterSchemaRequest clusterSchemaRequest = utilMethods.getSchema();
    String jsonReq = new ObjectMapper().writer().writeValueAsString(clusterSchemaRequest);

    ApiResponse apiResponse = ApiResponse.builder().result(ApiResultStatus.SUCCESS.value).build();

    when(schemaService.registerSchema(clusterSchemaRequest)).thenReturn(apiResponse);

    String response =
        mvc.perform(
                MockMvcRequestBuilders.post("/topics/postSchema")
                    .content(jsonReq)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    assertThat(response, CoreMatchers.containsString(ApiResultStatus.SUCCESS.value));
  }

  @Test
  public void postSchemaFail() throws Exception {
    ClusterSchemaRequest clusterSchemaRequest = utilMethods.getSchema();
    String jsonReq = new ObjectMapper().writer().writeValueAsString(clusterSchemaRequest);

    when(schemaService.registerSchema(clusterSchemaRequest))
        .thenThrow(new RuntimeException("Error registering schema"));

    String response =
        mvc.perform(
                MockMvcRequestBuilders.post("/topics/postSchema")
                    .content(jsonReq)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    assertThat(response, CoreMatchers.containsString(ApiResultStatus.SUCCESS.value));
  }
}
