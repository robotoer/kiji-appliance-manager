package org.kiji.appliance;

import java.io.IOException;

import org.apache.hadoop.yarn.exceptions.YarnException;

import org.kiji.appliance.record.ApplianceManagerConfiguration;
import org.kiji.appliance.record.ApplianceManagerId;
import org.kiji.appliance.record.ApplianceManagerStatus;

/**
 * Responsible for creating/starting/stopping service manager instances.
 */
public interface ApplianceManagerFactory {
  ApplianceManager connect(final ApplianceManagerId id);

  ApplianceManagerStatus start(final ApplianceManagerConfiguration managerConfiguration) throws IOException, InterruptedException, YarnException;
  ApplianceManagerStatus stop(final ApplianceManagerId id);
}
