package org.kiji.appliance.ipc;

import org.kiji.appliance.Appliance;
import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceStatus;

public class AvroApplianceClient implements Appliance {
  private final AvroApplianceManagerClient mManagerClient;

  public AvroApplianceClient(final AvroApplianceManagerClient managerClient) {
    mManagerClient = managerClient;
  }

  @Override
  public ApplianceId getApplianceId() {
    return null;
  }

  @Override
  public ApplianceConfiguration getConfiguration() {
    return null;
  }

  @Override
  public ApplianceStatus getStatus() {
    return null;
  }
}
