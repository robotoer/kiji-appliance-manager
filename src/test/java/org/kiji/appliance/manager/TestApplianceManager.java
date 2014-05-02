package org.kiji.appliance.manager;

public class TestApplianceManager {
//  private ApplianceManager mManager;
//  private ApplianceManagerFactory mApplianceManagerFactory;
//
//  @Before
//  public void setup() throws IOException {
//    // Start MiniYarnCluster.
////    ApplianceTest.startMiniYarnCluster();
//
//    // Start the ApplianceManager.
//    mApplianceManagerFactory = new ApplianceManagerFactory(getConfig());
//    mManager = mApplianceManagerFactory.start(new ApplianceManagerConfiguration(port, name, command, memory, cores));
//  }
//
//  @After
//  public void cleanup() {
//    // Stop the ApplianceManager.
//    new ApplianceManagerFactory(getConfig()).stop(mManager);
//
//    // Teardown MiniYarnCluster.
////    ApplianceTest.stopMiniYarnCluster(null);
//  }
//
//  @Test
//  public void testDeployUndeploy() throws IOException {
//    mManager.deploy(null, null, 0);
//    mManager.deploy(null, null, 0);
//
//    // Validate state.
//    mManager.listAppliances();
//    mManager.listApplianceInstances();
//
//
//    mManager.undeployAppliance("");
//
//    // Validate state.
//    mManager.listAppliances();
//    mManager.listApplianceInstances();
//
//
//    mManager.undeployApplianceInstance("");
//
//    // Validate state.
//    mManager.listAppliances();
//    mManager.listApplianceInstances();
//  }
//
//  @Test
//  public void testDeployOver() throws IOException {
//    mManager.deploy(null, null, 0);
//    mManager.deploy(null, null, 0);
//
//    // Validate state.
//    mManager.listAppliances();
//    mManager.listApplianceInstances();
//  }
//
//  @Test
//  public void testUndeployNonExistentAppliance() {
//    mManager.undeployAppliance("");
//
//    // Validate state.
//    mManager.listAppliances();
//    mManager.listApplianceInstances();
//  }
//
//  public void testUndeployNonExistentInstance() {
//    mManager.undeployApplianceInstance("");
//
//    // Validate state.
//    mManager.listAppliances();
//    mManager.listApplianceInstances();
//  }
//
//  @Test
//  public void testList() throws IOException {
//    mManager.deploy(null, null, 0);
//    mManager.deploy(null, null, 0);
//
//    mManager.listAppliances();
//    mManager.listApplianceInstances();
//  }
}
