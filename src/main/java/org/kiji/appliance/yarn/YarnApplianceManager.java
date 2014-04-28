package org.kiji.appliance.yarn;

import java.io.IOException;
import java.util.List;

import org.kiji.appliance.Appliance;
import org.kiji.appliance.ApplianceManager;
import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceId;
import org.kiji.appliance.record.ApplianceInstanceStatus;
import org.kiji.appliance.record.ApplianceStatus;

/**
 * Delegates calls to YarnApplianceMaster via an Http pipe.
 */
public class YarnApplianceManager implements ApplianceManager {
  @Override
  public Appliance connect(final ApplianceId id) {
    return null;
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
}
