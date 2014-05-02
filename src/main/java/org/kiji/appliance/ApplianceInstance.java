package org.kiji.appliance;

import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceConfiguration;
import org.kiji.appliance.record.ApplianceInstanceId;
import org.kiji.appliance.record.ApplianceInstanceStatus;

/**
 * Represents a connection to a service executor.
 */
public interface ApplianceInstance {
  ApplianceInstanceId getInstanceId();
  ApplianceId getApplianceId();
  ApplianceInstanceConfiguration getConfiguration();
  ApplianceInstanceStatus getStatus();
}
