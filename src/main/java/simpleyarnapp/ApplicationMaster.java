package simpleyarnapp;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Objects;
//import org.apache.curator.RetryPolicy;
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.retry.ExponentialBackoffRetry;
//import org.apache.curator.x.discovery.ServiceDiscovery;
//import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
//import org.apache.curator.x.discovery.ServiceInstance;
//import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import org.kiji.appliance.Appliance;
import org.kiji.appliance.ApplianceManager;
import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceId;
import org.kiji.appliance.record.ApplianceInstanceStatus;
import org.kiji.appliance.record.ApplianceStatus;
import org.kiji.appliance.yarn.YarnApplianceMaster;

public class ApplicationMaster implements ApplianceManager {
  public static final String CONNECT_IS_NOT_SUPPORTED_MSG = "Connect is not supported";
  public static final String BASE_SERVICE_DISCOVERY_PATH = "/org/kiji/services";
  public static final int YARN_HEARTBEAT_INTERVAL_MS = 500;
  public static final String CURATOR_SERVICE_NAME = "service-master";
//  public static final RetryPolicy CURATOR_RETRY_POLICY = new ExponentialBackoffRetry(1000, 3);

  private final String mMasterAddress;
  private final int mMasterPort;
  private final AMRMClientAsync<ContainerRequest> mResourceManagerClient;
  private final NMClient mNodeManagerClient;
//  private final CuratorFramework mCuratorClient;
//  private final ServiceInstance<YarnApplianceMaster.ServiceMasterDetails> mThisInstance;
//  private final JsonInstanceSerializer<YarnApplianceMaster.ServiceMasterDetails> mJsonSerializer;

//  public static void main(String[] args) throws Exception {
//
//    final String command = args[0];
//    final int n = Integer.valueOf(args[1]);
//
//    // Initialize clients to ResourceManager and NodeManagers
//    Configuration conf = new YarnConfiguration();
//
//    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
//    rmClient.init(conf);
//    rmClient.start();
//
//    NMClient nmClient = NMClient.createNMClient();
//    nmClient.init(conf);
//    nmClient.start();
//
//    // Register with ResourceManager
//    System.out.println("registerApplicationMaster 0");
//    rmClient.registerApplicationMaster("", 0, "");
//    System.out.println("registerApplicationMaster 1");
//
//    // Priority for worker containers - priorities are intra-application
//    Priority priority = Records.newRecord(Priority.class);
//    priority.setPriority(0);
//
//    // Resource requirements for worker containers
//    Resource capability = Records.newRecord(Resource.class);
//    capability.setMemory(128);
//    capability.setVirtualCores(1);
//
//    // Make container requests to ResourceManager
//    for (int i = 0; i < n; ++i) {
//      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
//      System.out.println("Making res-req " + i);
//      rmClient.addContainerRequest(containerAsk);
//    }
//
//    // Obtain allocated containers and launch
//    int allocatedContainers = 0;
//    while (allocatedContainers < n) {
//      AllocateResponse response = rmClient.allocate(0);
//      for (Container container : response.getAllocatedContainers()) {
//        ++allocatedContainers;
//
//        // Launch container by create ContainerLaunchContext
//        ContainerLaunchContext ctx =
//            Records.newRecord(ContainerLaunchContext.class);
//        ctx.setCommands(
//            Collections.singletonList(
//                command +
//                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
//                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
//                ));
//        System.out.println("Launching container " + allocatedContainers);
//        nmClient.startContainer(container, ctx);
//      }
//      Thread.sleep(100);
//    }
//
//    // Now wait for containers to complete
//    int completedContainers = 0;
//    while (completedContainers < n) {
//      AllocateResponse response = rmClient.allocate(completedContainers/n);
//      for (ContainerStatus status : response.getCompletedContainersStatuses()) {
//        ++completedContainers;
//        System.out.println("Completed container " + completedContainers);
//      }
//      Thread.sleep(100);
//    }
//
//    // Un-register with ResourceManager
//    rmClient.unregisterApplicationMaster(
//        FinalApplicationStatus.SUCCEEDED, "", "");
//  }


  public ApplicationMaster(
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
//          .<YarnApplianceMaster.ServiceMasterDetails>builder()
//          .id(masterId)
//          .name(CURATOR_SERVICE_NAME)
//          .address(masterAddress)
//          .port(masterPort)
//          .payload(new YarnApplianceMaster.ServiceMasterDetails())
//          .build();
//
//      mJsonSerializer =
//          new JsonInstanceSerializer<YarnApplianceMaster.ServiceMasterDetails>(YarnApplianceMaster.ServiceMasterDetails.class);
//    }
  }

//  private ServiceDiscovery<YarnApplianceMaster.ServiceMasterDetails> getServiceDiscovery() {
//    return ServiceDiscoveryBuilder
//        .builder(YarnApplianceMaster.ServiceMasterDetails.class)
//        .client(mCuratorClient)
//        .basePath(BASE_SERVICE_DISCOVERY_PATH)
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
//    final ServiceDiscovery<YarnApplianceMaster.ServiceMasterDetails> serviceDiscovery = getServiceDiscovery();
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
    // Unregister with Curator's service discovery mechanism.
//    final ServiceDiscovery<YarnApplianceMaster.ServiceMasterDetails> serviceDiscovery = getServiceDiscovery();
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
    throw new UnsupportedOperationException(CONNECT_IS_NOT_SUPPORTED_MSG);
  }

  @Override
  public ApplianceStatus deploy(final ApplianceConfiguration configuration) throws IOException {
    return null;
  }

  @Override
  public ApplianceStatus undeployAppliance(final ApplianceId id) {
    return null;
  }

  @Override
  public ApplianceInstanceStatus undeployApplianceInstance(final ApplianceInstanceId id) {
    return null;
  }

  @Override
  public List<ApplianceId> listAppliances() {
    return null;
  }

  @Override
  public List<ApplianceInstanceId> listApplianceInstances(final ApplianceId appliance) {
    return null;
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

  public static void main(final String[] args) throws Exception {
    final ApplicationMaster applicationMaster = new ApplicationMaster(
        "appliance-master-1",
        NetUtils.getHostname(),
        67453,
        "localhost:2181",
        new YarnConfiguration()
    );

    System.out.println(String.format("Starting ApplicationMaster: %s", applicationMaster.toString()));
    applicationMaster.start();
    System.out.println("Sleeping for 10 seconds.");
    Thread.sleep(10000);
    System.out.println("Done sleeping for 10 seconds.");
    System.out.println("Stopping ApplicationMaster...");
    applicationMaster.stop();
    System.out.println("Stopped ApplicationMaster.");
  }
}
