package org.kiji.appliance;

import java.io.Closeable;
import java.util.List;

import org.kiji.appliance.record.ApplianceDirectoryId;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceId;
import org.kiji.appliance.record.ApplianceManagerId;

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
