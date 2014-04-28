package org.kiji.appliance;

import java.io.IOException;
import java.util.List;

import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceId;
import org.kiji.appliance.record.ApplianceInstanceStatus;
import org.kiji.appliance.record.ApplianceStatus;

/**
 * The service manager is responsible for coordinating the deployment/undeployment of services.
 */
public interface ApplianceManager {
  Appliance connect(final ApplianceId id);

  ApplianceStatus deploy(final ApplianceConfiguration configuration) throws IOException;
  ApplianceStatus undeployAppliance(final ApplianceId id);
  ApplianceInstanceStatus undeployApplianceInstance(final ApplianceInstanceId id);

  List<ApplianceId> listAppliances();
  List<ApplianceInstanceId> listApplianceInstances(final ApplianceId appliance);
}
