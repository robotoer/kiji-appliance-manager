package org.kiji.appliance.record;

public class ApplianceStatus {
  private final ApplianceId mApplianceId;

  public ApplianceStatus(final ApplianceId applianceId) {
    mApplianceId = applianceId;
  }

  public ApplianceId getApplianceId() {
    return mApplianceId;
  }
}
