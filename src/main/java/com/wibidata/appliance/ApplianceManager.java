package com.wibidata.appliance;

import java.io.IOException;
import java.util.List;

import com.wibidata.appliance.record.ApplianceConfiguration;
import com.wibidata.appliance.record.ApplianceId;
import com.wibidata.appliance.record.ApplianceInstanceId;
import com.wibidata.appliance.record.ApplianceInstanceStatus;
import com.wibidata.appliance.record.ApplianceManagerId;
import com.wibidata.appliance.record.ApplianceManagerStatus;
import com.wibidata.appliance.record.ApplianceStatus;

/**
 * The service manager is responsible for coordinating the deployment/undeployment of services.
 */
public interface ApplianceManager {
  ApplianceManagerId getId();

  ApplianceStatus deploy(final ApplianceConfiguration configuration) throws IOException;
  ApplianceStatus undeployAppliance(final ApplianceId id) throws IOException;
  // TODO: Should this exist? Makes the api unsymmetric.
  ApplianceInstanceStatus undeployApplianceInstance(final ApplianceInstanceId id) throws IOException;

  ApplianceManagerStatus getStatus();
  ApplianceStatus getApplianceStatus(final ApplianceId id);
  ApplianceInstanceStatus getApplianceInstanceStatus(final ApplianceInstanceId id);

  List<ApplianceId> listAppliances() throws IOException;
  List<ApplianceInstanceId> listApplianceInstances(final ApplianceId appliance) throws IOException;
}
