package org.kiji.appliance.yarn;

import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.kiji.appliance.ApplianceManager;
import org.kiji.appliance.ApplianceManagerFactory;
import org.kiji.appliance.YarnClientTest;
import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceConfiguration;
import org.kiji.appliance.record.ApplianceInstanceId;
import org.kiji.appliance.record.ApplianceInstanceStatus;
import org.kiji.appliance.record.ApplianceManagerConfiguration;
import org.kiji.appliance.record.ApplianceManagerId;
import org.kiji.appliance.record.ApplianceManagerStatus;
import org.kiji.appliance.record.ApplianceStatus;

public class TestYarnApplianceManager extends YarnClientTest {
  public static final ApplianceConfiguration DUMMY_APPLICATION_CONF_1 = new ApplianceConfiguration(
      new ApplianceInstanceConfiguration("test-appliance-1", "watch date"),
      1
  );
  private static final ApplianceConfiguration DUMMY_APPLICATION_CONF_2 = new ApplianceConfiguration(
      new ApplianceInstanceConfiguration("test-appliance-2", "watch date"),
      2
  );

  private ApplianceManagerFactory mManagerFactory;

  @Before
  public void setUp() throws Exception {
    mManagerFactory = new YarnApplianceManagerFactory(getConfig());
  }

  public ApplianceManagerConfiguration generateManagerConfiguration() {
    // TODO: Give this real values.
    return new ApplianceManagerConfiguration(
        String.format("appliance-manager-%s", UUID.randomUUID().toString()),
        64,
        65893,
        1,
        "localhost:2181",
        Lists.<Path>newArrayList()
    );
  }

//  @Test
  @Ignore
  public void testGetStatus() throws Exception {
    final ApplianceManagerStatus managerStatus = mManagerFactory.start(generateManagerConfiguration());
    final ApplianceManager manager = mManagerFactory.connect(managerStatus.getManagerId());
    final ApplianceManagerId managerId = manager.getId();

    // Test getting the status of a running ApplianceManager. Should return valid status.
    {
      final ApplianceManagerStatus status = manager.getStatus();
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
        manager.getStatus();
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.MANAGER_NOT_RUNNING_MSG, exception.getMessage());
    }
  }

//  @Test
  @Ignore
  public void testGetApplianceStatus() throws Exception {
    final ApplianceManagerStatus managerStatus = mManagerFactory.start(generateManagerConfiguration());
    final ApplianceManager manager = mManagerFactory.connect(managerStatus.getManagerId());
    final ApplianceId nonExistentApplianceId = new ApplianceId();

    // Test getting the status of a non-existent Appliance.
    {
      IllegalStateException exception = null;
      try {
        manager.getApplianceStatus(nonExistentApplianceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.NONEXISTENT_APPLIANCE_ID_MSG, exception.getMessage());
    }

    // Test getting the status of a deployed Appliance.
    final ApplianceId dummyApplianceId = manager.deploy(DUMMY_APPLICATION_CONF_1).getApplianceId();
    {
      final ApplianceStatus status = manager.getApplianceStatus(dummyApplianceId);

      Assert.assertEquals(dummyApplianceId, status.getApplianceId());
      Assert.assertEquals(DUMMY_APPLICATION_CONF_1.getInstanceCount(), status.getInstanceCount());
      Assert.assertEquals(DUMMY_APPLICATION_CONF_1.getName(), status.getName());
    }

    // Test getting the status of an undeployed Appliance.
    {
      manager.undeployAppliance(dummyApplianceId);
      IllegalStateException exception = null;
      try {
        manager.getApplianceStatus(dummyApplianceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.NONEXISTENT_APPLIANCE_ID_MSG, exception.getMessage());
    }

    // Test getting the status of an Appliance when the manager is not longer running.
    {
      mManagerFactory.stop(manager.getId());
      IllegalStateException exception = null;
      try {
        manager.getApplianceStatus(dummyApplianceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.MANAGER_NOT_RUNNING_MSG, exception.getMessage());
    }
  }

//  @Test
  @Ignore
  public void testGetApplianceInstanceStatus() throws Exception {
    final ApplianceManagerStatus managerStatus = mManagerFactory.start(generateManagerConfiguration());
    final ApplianceManager manager = mManagerFactory.connect(managerStatus.getManagerId());
    final ApplianceInstanceId nonExistentApplianceInstanceId = new ApplianceInstanceId();

    // Test getting the status of a non-existent Appliance.
    {
      IllegalStateException exception = null;
      try {
        manager.getApplianceInstanceStatus(nonExistentApplianceInstanceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.NONEXISTENT_APPLIANCE_INSTANCE_ID_MSG, exception.getMessage());
    }

    // Test getting the status of a deployed Appliance.
    final ApplianceId dummyApplianceId = manager.deploy(DUMMY_APPLICATION_CONF_1).getApplianceId();
    final ApplianceInstanceId dummyApplianceInstanceId = manager.listApplianceInstances(dummyApplianceId).get(0);
    {
      final ApplianceInstanceStatus status = manager.getApplianceInstanceStatus(dummyApplianceInstanceId);

      Assert.assertEquals(dummyApplianceInstanceId, status.getInstanceId());
      // TODO: Validate other things.
    }

    // Test getting the status of an undeployed Appliance.
    {
      manager.undeployAppliance(dummyApplianceId);
      IllegalStateException exception = null;
      try {
        manager.getApplianceInstanceStatus(dummyApplianceInstanceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.NONEXISTENT_APPLIANCE_INSTANCE_ID_MSG, exception.getMessage());
    }

    // Test getting the status of an Appliance when the manager is not longer running.
    {
      mManagerFactory.stop(manager.getId());
      IllegalStateException exception = null;
      try {
        manager.getApplianceInstanceStatus(dummyApplianceInstanceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.MANAGER_NOT_RUNNING_MSG, exception.getMessage());
    }
  }

//  @Test
  @Ignore
  public void testListAppliances() throws Exception {
    final ApplianceManagerStatus managerStatus = mManagerFactory.start(generateManagerConfiguration());
    final ApplianceManager manager = mManagerFactory.connect(managerStatus.getManagerId());

    // Test listing appliances with none deployed.
    {
      final List<ApplianceId> appliances = manager.listAppliances();
      Assert.assertTrue(
          String.format("No appliances should be listed as none have been deployed. Listed: %s", appliances),
          appliances.isEmpty()
      );
    }

    // Test listing appliances with some deployed.
    final ApplianceStatus appliance1Status = manager.deploy(DUMMY_APPLICATION_CONF_1);
    final ApplianceStatus appliance2Status = manager.deploy(DUMMY_APPLICATION_CONF_2);
    {
      final List<ApplianceId> appliances = manager.listAppliances();
      Assert.assertEquals(2, appliances.size());
      Assert.assertTrue(
          String.format("Didn't list %s.", appliance1Status.getApplianceId()),
          appliances.contains(appliance1Status.getApplianceId())
      );
      Assert.assertTrue(
          String.format("Didn't list %s.", appliance2Status.getApplianceId()),
          appliances.contains(appliance2Status.getApplianceId())
      );
    }

    // Test listing appliances after some have been undeployed.
    {
      manager.undeployAppliance(appliance1Status.getApplianceId());
      final List<ApplianceId> appliances = manager.listAppliances();
      Assert.assertEquals(1, appliances.size());
      Assert.assertTrue(
          String.format("Didn't list %s.", appliance2Status.getApplianceId()),
          appliances.contains(appliance2Status.getApplianceId())
      );
    }

    // Test listing appliances after the manager has been stopped.
    {
      mManagerFactory.stop(manager.getId());
      IllegalStateException exception = null;
      try {
        manager.listAppliances();
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.MANAGER_NOT_RUNNING_MSG, exception.getMessage());
    }
  }

//  @Test
  @Ignore
  public void testListApplianceInstances() throws Exception {
    final ApplianceManagerStatus managerStatus = mManagerFactory.start(generateManagerConfiguration());
    final ApplianceManager manager = mManagerFactory.connect(managerStatus.getManagerId());
    final ApplianceId nonExistentApplianceId = new ApplianceId();

    // Test listing appliance instances of a non-existent appliance.
    {
      IllegalStateException exception = null;
      try {
        manager.listApplianceInstances(nonExistentApplianceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }

      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.NONEXISTENT_APPLIANCE_ID_MSG, exception.getMessage());
    }

    // Test listing appliance instances of a deployed appliance.
    final ApplianceStatus applianceStatus = manager.deploy(DUMMY_APPLICATION_CONF_2);
    {
      final List<ApplianceInstanceId> applianceInstances =
          manager.listApplianceInstances(applianceStatus.getApplianceId());
      Assert.assertEquals(2, applianceInstances.size());
      // TODO: Validate more.
    }

    // Test listing appliance instances of an undeployed appliance.
    {
      manager.undeployAppliance(applianceStatus.getApplianceId());

      IllegalStateException exception = null;
      try {
        manager.listApplianceInstances(applianceStatus.getApplianceId());
      } catch (final IllegalStateException ise) {
        exception = ise;
      }

      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.NONEXISTENT_APPLIANCE_ID_MSG, exception.getMessage());
    }

    // Test listing appliance instances after the manager has been stopped.
    {
      mManagerFactory.stop(manager.getId());
      IllegalStateException exception = null;
      try {
        manager.listApplianceInstances(applianceStatus.getApplianceId());
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.MANAGER_NOT_RUNNING_MSG, exception.getMessage());
    }
  }

//  @Test
  @Ignore
  public void testDeploy() throws Exception {
    final ApplianceManagerStatus managerStatus = mManagerFactory.start(generateManagerConfiguration());
    final ApplianceManager manager = mManagerFactory.connect(managerStatus.getManagerId());

    // Test deploying applications.
    {
      final ApplianceStatus appliance1Status = manager.deploy(DUMMY_APPLICATION_CONF_1);
      Assert.assertEquals(
          DUMMY_APPLICATION_CONF_1.getInstanceCount(),
          appliance1Status.getInstanceCount()
      );
      final ApplianceStatus appliance2Status = manager.deploy(DUMMY_APPLICATION_CONF_2);
      Assert.assertEquals(
          DUMMY_APPLICATION_CONF_2.getInstanceCount(),
          appliance2Status.getInstanceCount()
      );

      // Validate that the deployment went correctly.
      final List<ApplianceId> appliances = manager.listAppliances();
      Assert.assertEquals(2, appliances.size());
      Assert.assertTrue(
          String.format("Didn't list %s.", appliance1Status.getApplianceId()),
          appliances.contains(appliance1Status.getApplianceId())
      );
      Assert.assertTrue(
          String.format("Didn't list %s.", appliance2Status.getApplianceId()),
          appliances.contains(appliance2Status.getApplianceId())
      );
      Assert.assertEquals(
          DUMMY_APPLICATION_CONF_1.getInstanceCount(),
          manager.listApplianceInstances(appliance1Status.getApplianceId()).size()
      );
      Assert.assertEquals(
          DUMMY_APPLICATION_CONF_2.getInstanceCount(),
          manager.listApplianceInstances(appliance2Status.getApplianceId()).size()
      );
    }

    // Test deploying an application with duplicate configuration (should fail with exception).
    {
      IllegalStateException exception = null;
      try {
        manager.deploy(DUMMY_APPLICATION_CONF_1);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.DUPLICATE_APPLIANCE_NAME_MSG, exception.getMessage());
    }

    // Test deploying after the manager has been stopped.
    {
      mManagerFactory.stop(manager.getId());
      IllegalStateException exception = null;
      try {
        manager.deploy(DUMMY_APPLICATION_CONF_1);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.MANAGER_NOT_RUNNING_MSG, exception.getMessage());
    }
  }

//  @Test
  @Ignore
  public void testUndeployAppliance() throws Exception {
    final ApplianceManagerStatus managerStatus = mManagerFactory.start(generateManagerConfiguration());
    final ApplianceManager manager = mManagerFactory.connect(managerStatus.getManagerId());
    final ApplianceId nonExistentApplianceId = new ApplianceId();

    // Test undeploying a non-existent appliance.
    {
      IllegalStateException exception = null;
      try {
        manager.undeployAppliance(nonExistentApplianceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.NONEXISTENT_APPLIANCE_ID_MSG, exception.getMessage());
    }

    // Test undeploying appliances.
    final ApplianceStatus applianceStatus = manager.deploy(DUMMY_APPLICATION_CONF_1);
    {
      final List<ApplianceId> appliances = manager.listAppliances();
      Assert.assertEquals(1, appliances.size());
      Assert.assertTrue(
          "Appliance wasn't deployed correctly.",
          appliances.contains(applianceStatus.getApplianceId())
      );

      final ApplianceStatus applianceStatus1 =
          manager.undeployAppliance(applianceStatus.getApplianceId());

      Assert.assertEquals(applianceStatus1.getApplianceId(), applianceStatus.getApplianceId());
      Assert.assertTrue(
          "Appliance wasn't undeployed correctly.",
          manager.listAppliances().isEmpty()
      );
    }

    // Test undeploying after the manager has been stopped.
    {
      mManagerFactory.stop(manager.getId());
      IllegalStateException exception = null;
      try {
        manager.undeployAppliance(applianceStatus.getApplianceId());
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.MANAGER_NOT_RUNNING_MSG, exception.getMessage());
    }
  }

//  @Test
  @Ignore
  public void testUndeployInstance() throws Exception {
    final ApplianceManagerStatus managerStatus = mManagerFactory.start(generateManagerConfiguration());
    final ApplianceManager manager = mManagerFactory.connect(managerStatus.getManagerId());
    final ApplianceInstanceId nonExistentApplianceInstanceId = new ApplianceInstanceId();

    // Test undeploying a non-existent appliance instance.
    {
      IllegalStateException exception = null;
      try {
        manager.undeployApplianceInstance(nonExistentApplianceInstanceId);
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.NONEXISTENT_APPLIANCE_INSTANCE_ID_MSG, exception.getMessage());
    }

    // Test undeploying appliance instances.
    final ApplianceStatus applianceStatus = manager.deploy(DUMMY_APPLICATION_CONF_2);
    final List<ApplianceInstanceId> instances =
        manager.listApplianceInstances(applianceStatus.getApplianceId());
    Assert.assertEquals(
        DUMMY_APPLICATION_CONF_2.getInstanceCount(),
        instances.size()
    );
    final ApplianceInstanceId instance = instances.get(0);
    {
      manager.undeployApplianceInstance(instance);

      final List<ApplianceInstanceId> instances1 =
          manager.listApplianceInstances(applianceStatus.getApplianceId());
      Assert.assertEquals(
          DUMMY_APPLICATION_CONF_2.getInstanceCount() - 1,
          instances1.size()
      );
      Assert.assertTrue("Failed to undeploy instance.", !instances1.contains(instance));
    }

    // Test undeploying after the manager has been stopped.
    {
      mManagerFactory.stop(manager.getId());
      IllegalStateException exception = null;
      try {
        manager.undeployApplianceInstance(instances.get(1));
      } catch (final IllegalStateException ise) {
        exception = ise;
      }
      Assert.assertNotNull("An exception should have been thrown.", exception);
      Assert.assertEquals(YarnApplianceMaster.MANAGER_NOT_RUNNING_MSG, exception.getMessage());
    }
  }
}
