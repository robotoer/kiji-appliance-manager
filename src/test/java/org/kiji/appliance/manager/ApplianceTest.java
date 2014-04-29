package org.kiji.appliance.manager;

import java.net.URL;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.appliance.ApplianceManager;
import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceManagerConfiguration;
import org.kiji.appliance.record.ApplianceManagerStatus;
import org.kiji.appliance.yarn.YarnApplianceManagerFactory;

public class ApplianceTest {
  private static final Logger LOG = LoggerFactory.getLogger(ApplianceTest.class);

  private MiniYARNCluster mCluster;

  public MiniYARNCluster getCluster() {
    return mCluster;
  }

  public YarnConfiguration getConfig() {
    return new YarnConfiguration(mCluster.getConfig());
  }

//  @Before
  public void startMiniYarnCluster() throws Exception {
    final YarnConfiguration configuration = new YarnConfiguration();
    configuration.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
    configuration.setClass(
        YarnConfiguration.RM_SCHEDULER,
        FifoScheduler.class,
        ResourceScheduler.class
    );

    mCluster = new MiniYARNCluster("tempTest", 2, 1, 1);
    mCluster.init(configuration);
    mCluster.start();
  }

//  @After
  public void stopMiniYarnCluster() {
    mCluster.stop();
  }

  public static void main(final String[] args) throws Exception {
//    // Setup ApplianceProvider.
//    {
//      // Setup a zookeeper connection.
//      mCuratorClient =
//          CuratorFrameworkFactory.newClient(curatorUrl, CURATOR_RETRY_POLICY);
//
//      mThisInstance = ApplianceInstance
//          .<YarnApplianceMaster.ApplianceMasterDetails>builder()
//          .id(masterId)
//          .name(CURATOR_SERVICE_NAME)
//          .address(masterAddress)
//          .port(masterPort)
//          .build();
//
//      mJsonSerializer =
//          new JsonInstanceSerializer<YarnApplianceMaster.ApplianceMasterDetails>(YarnApplianceMaster.ApplianceMasterDetails.class);
//    }
//
//    // Register with Curator's appliance discovery mechanism.
//    ApplianceDiscoveryBuilder
//        .builder(YarnApplianceMaster.ApplianceMasterDetails.class)
//        .client(mCuratorClient)
//        .basePath(BASE_SERVICE_DISCOVERY_PATH)
//        .serializer(mJsonSerializer)
//        .thisInstance(mThisInstance)
//        .build();
//
//    final ApplianceDiscovery<YarnApplianceMaster.ApplianceMasterDetails> applianceDiscovery = getApplianceDiscovery();
//    try {
//      // Does this actually register the application master?
//      applianceDiscovery.start();
//      applianceDiscovery.registerAppliance(mThisInstance);
//    } finally {
//      applianceDiscovery.close();
//    }

    simple(new YarnConfiguration());
  }

  public static void simple(final YarnConfiguration baseConfig) throws Exception {
    final String appName = "test-yarn-application";
//    final String appCommand = "echo hello world";
    final int appMemory = 256;
    final int appPort = 8080;
    final int appCores = 1;
    final String curatorAddress = "localhost:2181";

    final YarnApplianceManagerFactory managerFactory = new YarnApplianceManagerFactory(baseConfig);

    final ApplianceManagerConfiguration managerConfiguration =
        new ApplianceManagerConfiguration(appName, appMemory, appPort, appCores, curatorAddress);
    final ApplianceManagerStatus managerId = managerFactory.start(managerConfiguration);
//    final ApplianceManager manager = managerFactory.connect(managerId.getManagerId());
//    manager.listAppliances();
  }

  @Test
  public void tempTest() throws Exception {
//    final String appName = "test-yarn-application";
//    final String appCommand = "echo hello world";
//    final int appMemory = 64;
//    final int appPort = 8080;
//    final int appCores = 1;

    startMiniYarnCluster();

//    LOG.info("Sleeping to wait for YARN to start...");
//    Thread.sleep(10000);
//    LOG.info("Done sleeping to wait for YARN to start...");

//    final YarnConfiguration baseConfig = getConfig();
//    final YarnApplianceManagerFactory managerFactory = new YarnApplianceManagerFactory(baseConfig);
//
//    final ApplianceManagerConfiguration managerConfiguration =
//        new ApplianceManagerConfiguration(appName, appCommand, appMemory, appPort, appCores);
//    final ApplianceManager manager = managerFactory.start(managerConfiguration);
////    manager.listAppliances();

    simple(getConfig());

//    Thread.sleep(10000);

    stopMiniYarnCluster();
  }




































//  public static void test() throws Exception {
//    // Deploy a appliance (java api).
//    final ApplianceConfiguration applianceConfiguration = null;
//    final URL jarUrl = null;
//
//    final ApplianceManager applianceManager = null;
//
//    applianceManager.deploy(
//        // Appliance configuration.
//        applianceConfiguration
//    );
//    // Deploy a appliance (cli api).
//    commandLine(
//        "deploy-appliance",
//
//        // Appliance configuration.
//        "--type=" + applianceConfiguration.getType(),
//
//        // Jar url.
//        "--jar-url=" + jarUrl.toString(),
//
//        // Number of instances.
//        "--instances=1"
//    );
//
//    // Undeploy a appliance (java api).
//    applianceManager.undeployAppliance(
//        "appliance-type"
//    );
//    applianceManager.undeployApplianceInstance(
//        "appliance-id"
//    );
//    // Undeploy a appliance (cli api).
//    commandLine("undeploy-appliance", applianceConfiguration.getType());
//    commandLine("undeploy-instance", "appliance-type.0");
//
//    // List deployed appliances (java api).
//    applianceManager.listAppliances();
//    applianceManager.listApplianceInstances();
//    // List deployed appliances (cli api).
//    commandLine("list-appliances");
//    commandLine("list-appliance-instances");
//  }
//
//  private static void commandLine(final String... arguments) {
//  }
}
