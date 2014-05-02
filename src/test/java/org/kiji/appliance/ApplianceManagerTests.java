package org.kiji.appliance;

import org.junit.Assert;
import org.junit.Test;

import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceConfiguration;
import org.kiji.appliance.record.ApplianceInstanceId;
import org.kiji.appliance.record.ApplianceInstanceStatus;
import org.kiji.appliance.record.ApplianceManagerId;
import org.kiji.appliance.record.ApplianceManagerStatus;
import org.kiji.appliance.record.ApplianceStatus;
import org.kiji.appliance.yarn.YarnApplianceMaster;

public abstract class ApplianceManagerTests {
  private final ApplianceManager mManager;
  private final ApplianceManagerFactory mManagerFactory;

  protected ApplianceManagerTests(
      final ApplianceManagerFactory managerFactory, final ApplianceManager manager
  ) {
    mManagerFactory = managerFactory;
    mManager = manager;
  }


  // getStatus
  //  - started
  //  - stopped
  // getApplianceStatus
  //  - deployed
  //  - deployed/undeployed
  //  - non-existent
  //  - manager stopped
  // getApplianceInstanceStatus
  //  - deployed
  //  - deployed/undeployed
  //  - non-existent
  //  - manager stopped

  // listAppliances
  //  - empty
  //  - deployed 1 appliance
  //  - deployed >1 appliances
  //  - deployed/undeployed
  //  - manager stopped
  // listApplianceInstances
  //  - non-existent
  //  - deployed 1 instance
  //  - deployed >1 instances
  //  - deployed/undeployed
  //  - manager stopped

  // deploy
  //  - deploy 1
  //  - deploy >1
  // undeploy (unpinned dimension: number deployed/undeployed)
  //  - deployed 1/undeploy 1
  //  - deployed >1/undeploy >1
  //  - deployed 1/undeploy >1


  @Test
  public void testGetStatus() throws Exception {
    final ApplianceManagerId managerId = mManager.getId();

    // Test getting the status of a running ApplianceManager. Should return valid status.
    {
      final ApplianceManagerStatus status = mManager.getStatus();
      Assert.assertEquals(
          "ApplianceManager should be running.",
          status.getManagerId(),
          managerId
      );
    }

    // Test getting the status of a stopped ApplianceManager. Should fail with an exception.
    {
      mManagerFactory.stop(managerId);
      IllegalStateException exception = null;
      try {
        mManager.getStatus();
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(exception.getMessage(), YarnApplianceMaster.MANAGER_NOT_RUNNING_MSG);
    }
  }

  public void testGetApplianceStatus() throws Exception {
    final ApplianceId nonExistentApplianceId = new ApplianceId();
    final ApplianceConfiguration dummyApplicationConf =
        new ApplianceConfiguration(
            new ApplianceInstanceConfiguration("test-appliance", "watch date"),
            1
        );

    // Test getting the status of a non-existent Appliance.
    {
      IllegalStateException exception = null;
      try {
        mManager.getApplianceStatus(nonExistentApplianceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(exception.getMessage(), YarnApplianceMaster.NONEXISTENT_APPLIANCE_ID_MSG);
    }

    // Test getting the status of a deployed Appliance.
    final ApplianceId dummyApplianceId = mManager.deploy(dummyApplicationConf).getApplianceId();
    {
      final ApplianceStatus status = mManager.getApplianceStatus(dummyApplianceId);

      Assert.assertEquals(dummyApplianceId, status.getApplianceId());
      Assert.assertEquals(dummyApplicationConf.getInstanceCount(), status.getInstanceCount());
      Assert.assertEquals(dummyApplicationConf.getName(), status.getName());
    }

    // Test getting the status of an undeployed Appliance.
    {
      mManager.undeployAppliance(dummyApplianceId);
      IllegalStateException exception = null;
      try {
        mManager.getApplianceStatus(dummyApplianceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(exception.getMessage(), YarnApplianceMaster.NONEXISTENT_APPLIANCE_ID_MSG);
    }

    // Test getting the status of an Appliance when the manager is not longer running.
    {
      mManagerFactory.stop(mManager.getId());
      IllegalStateException exception = null;
      try {
        mManager.getApplianceStatus(dummyApplianceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(exception.getMessage(), YarnApplianceMaster.MANAGER_NOT_RUNNING_MSG);
    }
  }

  public void testGetApplianceInstanceStatus() throws Exception {
    // getApplianceInstanceStatus
    //  - deployed
    //  - deployed/undeployed
    //  - non-existent
    //  - manager stopped
    final ApplianceInstanceId nonExistentApplianceInstanceId = new ApplianceInstanceId();
    final ApplianceConfiguration dummyApplicationConf =
        new ApplianceConfiguration(
            new ApplianceInstanceConfiguration("test-appliance", "watch date"),
            1
        );

    // Test getting the status of a non-existent Appliance.
    {
      IllegalStateException exception = null;
      try {
        mManager.getApplianceInstanceStatus(nonExistentApplianceInstanceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(exception.getMessage(), YarnApplianceMaster.NONEXISTENT_APPLIANCE_INSTANCE_ID_MSG);
    }

    // Test getting the status of a deployed Appliance.
    final ApplianceId dummyApplianceId = mManager.deploy(dummyApplicationConf).getApplianceId();
    final ApplianceInstanceId dummyApplianceInstanceId = mManager.listApplianceInstances(dummyApplianceId).get(0);
    {
      final ApplianceInstanceStatus status = mManager.getApplianceInstanceStatus(dummyApplianceInstanceId);

      Assert.assertEquals(dummyApplianceInstanceId, status.getInstanceId());
      // TODO: Validate other things.
    }

//    // Test getting the status of an undeployed Appliance.
//    {
//      mManager.undeployAppliance(dummyApplianceId);
//      IllegalStateException exception = null;
//      try {
//        mManager.getApplianceInstanceStatus(dummyApplianceId);
//      } catch (final IllegalStateException ise) {
//        exception = ise;
//      }
//      Assert.assertNotNull("An exception should have been thrown.", exception);
//      Assert.assertEquals(exception.getMessage(), YarnApplianceMaster.NONEXISTENT_APPLIANCE_ID_MSG);
//    }
//
//    // Test getting the status of an Appliance when the manager is not longer running.
//    {
//      mManagerFactory.stop(mManager.getId());
//      IllegalStateException exception = null;
//      try {
//        mManager.getApplianceInstanceStatus(dummyApplianceId);
//      } catch (final IllegalStateException ise) {
//        exception = ise;
//      }
//      Assert.assertNotNull("An exception should have been thrown.", exception);
//      Assert.assertEquals(exception.getMessage(), YarnApplianceMaster.MANAGER_NOT_RUNNING_MSG);
//    }
  }


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
