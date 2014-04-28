package org.kiji.appliance.yarn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Objects;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.appliance.Appliance;
import org.kiji.appliance.ApplianceInstance;
import org.kiji.appliance.ApplianceManager;
import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceId;
import org.kiji.appliance.record.ApplianceInstanceStatus;
import org.kiji.appliance.record.ApplianceStatus;

// Should be an ApplicationMaster.
//
// ApplicationManager:
//  - Setup:
//    - Integrate/connect to ResourceManager
//  - Normal:
//    - Listen to http admin APIs
//  - Cleanup:
//    - Deregister from ResourceManager
//
//  - Operations:
//    -
public class YarnApplianceMaster implements ApplianceManager {
//  public static final String YARN_SERVICE_MASTER_JAVA_FLAGS = "-Xmx256M -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1337";
  public static final String YARN_SERVICE_MASTER_JAVA_FLAGS = "-Xmx256M";
  private static final Logger LOG = LoggerFactory.getLogger(YarnApplianceMaster.class);

  public static final String BASE_SERVICE_DISCOVERY_PATH = "/org/kiji/services";
  public static final int YARN_HEARTBEAT_INTERVAL_MS = 500;
  public static final String CURATOR_SERVICE_NAME = "service-master";
  public static final RetryPolicy CURATOR_RETRY_POLICY = new ExponentialBackoffRetry(1000, 3);

  // Yarn fields.
  private final AMRMClientAsync<AMRMClient.ContainerRequest> mResourceManagerClient;
  private final NMClient mNodeManagerClient;

  // Curator fields.
  private final CuratorFramework mCuratorClient;
  private final InstanceSerializer<ServiceMasterDetails> mJsonSerializer;

  // Jetty fields.
  private final ServiceInstance<ServiceMasterDetails> mThisInstance;

  public YarnApplianceMaster(
      final String masterId,
      final String masterAddress,
      final int masterPort,
      final String curatorUrl,
      final YarnConfiguration yarnConf
  ) throws Exception {
    // Setup Yarn.
    {
//      final Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
//      final DataOutputBuffer dob = new DataOutputBuffer();
//      credentials.writeTokenStorageToStream(dob);
//      // Now remove the AM->RM token so that containers cannot access it.
//      final Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
//      LOG.info("Executing with tokens:");
//      while (iter.hasNext()) {
//        final Token<?> token = iter.next();
//        LOG.info(token.toString());
//        if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
//          iter.remove();
//        }
//      }
//      allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
//
//      // Create appSubmitterUgi and add original tokens to it
//      String appSubmitterUserName =
//          System.getenv(ApplicationConstants.Environment.USER.name());
//      appSubmitterUgi =
//          UserGroupInformation.createRemoteUser(appSubmitterUserName);
//      appSubmitterUgi.addCredentials(credentials);

      // Initialize ResourceManager and NodeManager. AMRMClientAsync is used here because it runs a
      // heartbeat thread.
      mResourceManagerClient = AMRMClientAsync.createAMRMClientAsync(YARN_HEARTBEAT_INTERVAL_MS, null);
      mResourceManagerClient.init(yarnConf);
      mResourceManagerClient.start();

      // TODO: Should these be closed?
      mNodeManagerClient = NMClient.createNMClient();
      mNodeManagerClient.init(yarnConf);
      mNodeManagerClient.start();
    }


    // Setup ServiceProvider.
    {
      // Setup a zookeeper connection.
      mCuratorClient =
          CuratorFrameworkFactory.newClient(curatorUrl, CURATOR_RETRY_POLICY);

      mThisInstance = ServiceInstance
          .<ServiceMasterDetails>builder()
          .id(masterId)
          .name(CURATOR_SERVICE_NAME)
          .address(masterAddress)
          .port(masterPort)
          .payload(new ServiceMasterDetails())
          .build();

      mJsonSerializer =
          new JsonInstanceSerializer<ServiceMasterDetails>(ServiceMasterDetails.class);
    }
  }

  private ServiceDiscovery<ServiceMasterDetails> getServiceDiscovery() {
    return ServiceDiscoveryBuilder
        .builder(ServiceMasterDetails.class)
        .client(mCuratorClient)
        .basePath(BASE_SERVICE_DISCOVERY_PATH)
        .serializer(mJsonSerializer)
        .thisInstance(mThisInstance)
        .build();
  }

  public void start() throws Exception {
    // Register with ResourceManager.
    // TODO: Are these supposed to not be blank values?
    LOG.info("Registering YarnApplianceMaster...");
    mResourceManagerClient.registerApplicationMaster("", 0, "");
    LOG.info("Registered YarnApplianceMaster...");

    // Register with Curator's service discovery mechanism.
    final ServiceDiscovery<ServiceMasterDetails> serviceDiscovery = getServiceDiscovery();
    try {
      // Does this actually register the application master?
      serviceDiscovery.start();
      serviceDiscovery.registerService(mThisInstance);

      final Collection<String> names = serviceDiscovery.queryForNames();

      names.size();
    } finally {
      serviceDiscovery.close();
    }
  }

  public void stop() throws Exception {
    // Unregister with Curator's service discovery mechanism.
    final ServiceDiscovery<ServiceMasterDetails> serviceDiscovery = getServiceDiscovery();
    try {
      serviceDiscovery.start();
      serviceDiscovery.unregisterService(mThisInstance);
    } finally {
      serviceDiscovery.close();
    }

    // Unregister with ResourceManager.
    LOG.info("Unregistering YarnApplianceMaster...");
    mResourceManagerClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
  }

  @Override
  public Appliance connect(final ApplianceId id) {
    return null;
  }

  @Override
  public ApplianceStatus deploy(
      final ApplianceConfiguration configuration
  ) {
    // Allocate new resource containers to execute the service.
    LOG.info("Received a deploy call!");
    return null;
  }

  @Override
  public ApplianceStatus undeployAppliance(final ApplianceId id) {
    // Get a handle to the required resource containers to kill their corresponding service.
    LOG.info("Received an undeploy call!");
    return null;
  }

  @Override
  public ApplianceInstanceStatus undeployApplianceInstance(final ApplianceInstanceId id) {
    // Get a handle to the required resource containers to kill their corresponding service instance.
    LOG.info("Received an undeploy call!");
    return null;
  }

  @Override
  public List<ApplianceInstanceId> listApplianceInstances(final ApplianceId appliance) {
    // Dump:
    //  - Services that the ApplianceManager is aware of.
    //  - Actual getStatus of the corresponding resource containers.
    LOG.info("Received a list call!");
    return null;
  }

  @Override
  public List<ApplianceId> listAppliances() {
    // Dump:
    //  - Instances that the Service is composed of.
    //  - Actual getStatus of the corresponding service instances.
    LOG.info("Received a list call!");
    return null;
  }

  // ApplicationMaster logic
  public static void main(final String[] args) throws Exception {
    final YarnConfiguration yarnConf = new YarnConfiguration();
    final String masterAddress = NetUtils.getHostname();

    // Parse cli arguments.
    final String masterId = args[0];
    final int masterPort = Integer.parseInt(args[1]);
    final String curatorAddress = args[2];
//    final String masterId = "service-master-1";
//    final int masterPort = 8080;
//    final String curatorAddress = "";

    final YarnApplianceMaster serviceMaster = new YarnApplianceMaster(
        masterId,
        masterAddress,
        masterPort,
        curatorAddress,
        yarnConf
    );
    LOG.info("Starting {}...", serviceMaster.toString());
    serviceMaster.start();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("mResourceManagerClient", mResourceManagerClient)
        .add("mNodeManagerClient", mNodeManagerClient)
        .add("mCuratorClient", mCuratorClient)
        .add("mJsonSerializer", mJsonSerializer)
        .add("mThisInstance", mThisInstance)
        .toString();
  }

  public static String prepareArgs(
      final String masterName,
      final int masterPort,
      final String curatorAddress
  ) {
    return String.format("%s %d %s", masterName, masterPort, curatorAddress);
  }

  public static class ServiceMasterDetails {
  }
}
