package org.kiji.appliance;

import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceStatus;

public interface Appliance {
  ApplianceId getApplianceId();
  ApplianceConfiguration getConfiguration();
  ApplianceStatus getStatus();
}
