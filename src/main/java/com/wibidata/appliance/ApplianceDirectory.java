package com.wibidata.appliance;

import java.io.Closeable;
import java.util.List;

import com.wibidata.appliance.record.ApplianceDirectoryId;
import com.wibidata.appliance.record.ApplianceId;
import com.wibidata.appliance.record.ApplianceInstanceId;
import com.wibidata.appliance.record.ApplianceManagerId;

public interface ApplianceDirectory extends Closeable {
  ApplianceDirectoryId getId();

  void putApplianceInstance(final ApplianceInstanceId id);
  void putAppliance(final ApplianceId id);
  void putApplianceManager(final ApplianceManagerId id);

  List<ApplianceInstanceId> getApplianceInstances(final ApplianceId id);
  List<ApplianceId> getAppliances(final ApplianceManagerId id);
  List<ApplianceManagerId> getApplianceManagers();

  // TODO: Think about adding a watcher api.
}
