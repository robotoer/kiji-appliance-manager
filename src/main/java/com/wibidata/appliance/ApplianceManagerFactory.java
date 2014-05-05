package com.wibidata.appliance;

import java.io.IOException;

import org.apache.hadoop.yarn.exceptions.YarnException;

import com.wibidata.appliance.record.ApplianceManagerConfiguration;
import com.wibidata.appliance.record.ApplianceManagerId;
import com.wibidata.appliance.record.ApplianceManagerStatus;

/**
 * Responsible for creating/starting/stopping service manager instances.
 */
public interface ApplianceManagerFactory {
  ApplianceManager connect(final ApplianceManagerId id);

  ApplianceManagerStatus start(final ApplianceManagerConfiguration managerConfiguration) throws IOException, InterruptedException, YarnException;
  ApplianceManagerStatus stop(final ApplianceManagerId id);
}
