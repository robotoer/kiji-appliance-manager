package org.kiji.appliance.yarn;

import java.util.List;

import com.google.common.base.Objects;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.appliance.Appliance;
import org.kiji.appliance.ApplianceManager;
import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceId;
import org.kiji.appliance.record.ApplianceInstanceStatus;
import org.kiji.appliance.record.ApplianceStatus;

public class YarnApplianceMaster implements ApplianceManager {
//  public static final String YARN_APPLIANCE_MASTER_JAVA_FLAGS = "-Xmx256M -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1337";
  public static final String YARN_APPLIANCE_MASTER_JAVA_FLAGS = "-Xmx256M";
  private static final Logger LOG = LoggerFactory.getLogger(YarnApplianceMaster.class);

  public static final String BASE_APPLIANCE_DISCOVERY_PATH = "/org/kiji/appliances";
  public static final int YARN_HEARTBEAT_INTERVAL_MS = 500;
  public static final String CURATOR_SERVICE_NAME = "appliance-master";
//  public static final RetryPolicy CURATOR_RETRY_POLICY = new ExponentialBackoffRetry(1000, 3);

  // Yarn connections.
  private final AMRMClientAsync<AMRMClient.ContainerRequest> mResourceManagerClient;
  private final NMClient mNodeManagerClient;

//  // Curator connections.
//  private final CuratorFramework mCuratorClient;
//  private final InstanceSerializer<ServiceMasterDetails> mJsonSerializer;
//  private final ServiceInstance<ServiceMasterDetails> mThisInstance;

  private final String mMasterAddress;
  private final int mMasterPort;

  public YarnApplianceMaster(
      final String masterId,
      final String masterAddress,
      final int masterPort,
      final String curatorUrl,
      final YarnConfiguration yarnConf
  ) throws Exception {
    mMasterAddress = masterAddress;
    mMasterPort = masterPort;
    // Setup Yarn.
    {
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

//    // Setup ServiceProvider.
//    {
//      // Setup a zookeeper connection.
//      mCuratorClient =
//          CuratorFrameworkFactory.newClient(curatorUrl, CURATOR_RETRY_POLICY);
//
//      mThisInstance = ServiceInstance
//          .<ServiceMasterDetails>builder()
//          .id(masterId)
//          .name(CURATOR_SERVICE_NAME)
//          .address(masterAddress)
//          .port(masterPort)
//          .payload(new ServiceMasterDetails())
//          .build();
//
//      mJsonSerializer =
//          new JsonInstanceSerializer<ServiceMasterDetails>(ServiceMasterDetails.class);
//    }
  }

//  private ServiceDiscovery<ServiceMasterDetails> getServiceDiscovery() {
//    return ServiceDiscoveryBuilder
//        .builder(ServiceMasterDetails.class)
//        .client(mCuratorClient)
//        .basePath(BASE_APPLIANCE_DISCOVERY_PATH)
//        .serializer(mJsonSerializer)
//        .thisInstance(mThisInstance)
//        .build();
//  }

  public void start() throws Exception {
    // Register with ResourceManager.
    // TODO: Are these supposed to not be blank values?
    System.out.println("Registering YarnApplianceMaster...");
    mResourceManagerClient.registerApplicationMaster(mMasterAddress, mMasterPort, "");
    System.out.println("Registered YarnApplianceMaster...");

//    // Register with Curator's service discovery mechanism.
//    final ServiceDiscovery<ServiceMasterDetails> serviceDiscovery = getServiceDiscovery();
//    try {
//      // Does this actually register the application master?
//      serviceDiscovery.start();
//      serviceDiscovery.registerService(mThisInstance);
//
//      final Collection<String> names = serviceDiscovery.queryForNames();
//
//      names.size();
//    } finally {
//      serviceDiscovery.close();
//    }
  }

  public void stop() throws Exception {
//    // Unregister with Curator's service discovery mechanism.
//    final ServiceDiscovery<ServiceMasterDetails> serviceDiscovery = getServiceDiscovery();
//    try {
//      serviceDiscovery.start();
//      serviceDiscovery.unregisterService(mThisInstance);
//    } finally {
//      serviceDiscovery.close();
//    }

    // Unregister with ResourceManager.
    System.out.println("Unregistering YarnApplianceMaster...");
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
    System.out.println("Received a deploy call!");
    return null;
  }

  @Override
  public ApplianceStatus undeployAppliance(final ApplianceId id) {
    // Get a handle to the required resource containers to kill their corresponding service.
    System.out.println("Received an undeploy call!");
    return null;
  }

  @Override
  public ApplianceInstanceStatus undeployApplianceInstance(final ApplianceInstanceId id) {
    // Get a handle to the required resource containers to kill their corresponding service instance.
    System.out.println("Received an undeploy call!");
    return null;
  }

  @Override
  public List<ApplianceInstanceId> listApplianceInstances(final ApplianceId appliance) {
    // Dump:
    //  - Services that the ApplianceManager is aware of.
    //  - Actual getStatus of the corresponding resource containers.
    System.out.println("Received a list call!");
    return null;
  }

  @Override
  public List<ApplianceId> listAppliances() {
    // Dump:
    //  - Instances that the Service is composed of.
    //  - Actual getStatus of the corresponding service instances.
    System.out.println("Received a list call!");
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

    final YarnApplianceMaster applianceMaster = new YarnApplianceMaster(
        masterId,
        masterAddress,
        masterPort,
        curatorAddress,
        yarnConf
    );
    LOG.info("Starting {}...", applianceMaster.toString());

    System.out.println(String.format("Starting ApplianceMaster: %s", applianceMaster.toString()));
    applianceMaster.start();
    System.out.println("Sleeping for 10 seconds.");
    Thread.sleep(10000);
    System.out.println("Done sleeping for 10 seconds.");
    System.out.println("Stopping ApplianceMaster...");
    applianceMaster.stop();
    System.out.println("Stopped ApplianceMaster.");
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("mResourceManagerClient", mResourceManagerClient)
        .add("mNodeManagerClient", mNodeManagerClient)
//        .add("mCuratorClient", mCuratorClient)
//        .add("mJsonSerializer", mJsonSerializer)
//        .add("mThisInstance", mThisInstance)
        .toString();
  }

  public static String prepareArgs(
      final String masterName,
      final int masterPort,
      final String curatorAddress
  ) {
    return String.format("%s %d %s", masterName, masterPort, curatorAddress);
  }

  public String getMasterAddress() {
    return mMasterAddress;
  }

  public int getMasterPort() {
    return mMasterPort;
  }

  public static class ServiceMasterDetails {
  }
}
