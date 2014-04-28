package org.kiji.appliance.record;

public class ApplianceInstanceStatus {
  private final ApplianceInstanceId mInstanceId;

  public ApplianceInstanceStatus(final ApplianceInstanceId instanceId) {
    mInstanceId = instanceId;
  }

  public ApplianceInstanceId getInstanceId() {
    return mInstanceId;
  }
}
