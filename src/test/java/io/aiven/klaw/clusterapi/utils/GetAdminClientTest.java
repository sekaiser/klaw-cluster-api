package io.aiven.klaw.clusterapi.utils;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import io.aiven.klaw.clusterapi.models.KafkaSupportedProtocol;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.core.env.Environment;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AdminClient.class)
public class GetAdminClientTest {

  public static final String LOCALHOST_9092 = "localhost:9092";
  public static final String LOCALHOST_9093 = "localhost:9093";
  public static final String LOCALHOST = "localhost";
  @Mock Environment env;

  @Mock AdminClient adminClient;

  @Mock private ListTopicsResult listTopicsResult;

  @Mock private KafkaFuture<Set<String>> kafkaFuture;

  @Mock private HashMap<String, AdminClient> adminClientsMap;

  ClusterApiUtils getAdminClient;

  @Before
  public void setUp() throws Exception {
    getAdminClient = new ClusterApiUtils(env);
    ReflectionTestUtils.setField(getAdminClient, "adminClientsMap", adminClientsMap);
    ReflectionTestUtils.setField(getAdminClient, "env", env);
  }

  @Test
  @Ignore
  public void getAdminClient1() throws Exception {
    mockStatic(AdminClient.class);

    when(env.getProperty(any())).thenReturn("null");
    when(adminClient.listTopics()).thenReturn(listTopicsResult);
    when(listTopicsResult.names()).thenReturn(kafkaFuture);
    Set<String> setStr = new HashSet<>();
    when(kafkaFuture.get()).thenReturn(setStr);
    when(adminClientsMap.containsKey(LOCALHOST_9092)).thenReturn(false);
    BDDMockito.given(AdminClient.create(any(Properties.class))).willReturn(adminClient);

    AdminClient result =
        getAdminClient.getAdminClient(LOCALHOST_9092, KafkaSupportedProtocol.PLAINTEXT, "");
    assertNotNull(result);
  }

  @Test
  @Ignore
  public void getAdminClient2() throws Exception {
    mockStatic(AdminClient.class);

    when(env.getProperty(any())).thenReturn("true");
    when(adminClient.listTopics()).thenReturn(listTopicsResult);
    when(listTopicsResult.names()).thenReturn(kafkaFuture);
    Set<String> setStr = new HashSet<>();
    when(kafkaFuture.get()).thenReturn(setStr);
    BDDMockito.given(AdminClient.create(any(Properties.class))).willReturn(adminClient);

    AdminClient result =
        getAdminClient.getAdminClient(LOCALHOST_9092, KafkaSupportedProtocol.PLAINTEXT, "");
    assertNotNull(result);
  }

  @Test
  @Ignore
  public void getAdminClient3() throws Exception {
    mockStatic(AdminClient.class);

    when(env.getProperty(any())).thenReturn("false");
    BDDMockito.given(AdminClient.create(any(Properties.class))).willReturn(adminClient);
    when(adminClient.listTopics()).thenReturn(listTopicsResult);
    when(listTopicsResult.names()).thenReturn(kafkaFuture);
    Set<String> setStr = new HashSet<>();
    when(kafkaFuture.get()).thenReturn(setStr);

    AdminClient result =
        getAdminClient.getAdminClient(LOCALHOST_9092, KafkaSupportedProtocol.PLAINTEXT, "");
    assertNotNull(result);
  }

  @Test
  @Ignore
  public void getPlainProperties() {
    when(env.getProperty(any())).thenReturn("somevalue");
    Properties props = getAdminClient.getPlainProperties(LOCALHOST);
    assertEquals(LOCALHOST, props.getProperty("bootstrap.servers"));
  }

  @Test
  @Ignore
  public void getSslProperties() {
    when(env.getProperty(any())).thenReturn("somevalue");

    Properties props = getAdminClient.getSslProperties(LOCALHOST_9093, "");
    assertEquals(LOCALHOST_9093, props.getProperty("bootstrap.servers"));
  }
}
