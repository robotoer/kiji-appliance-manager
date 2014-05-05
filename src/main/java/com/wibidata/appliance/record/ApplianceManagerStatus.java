package com.wibidata.appliance.record;

public class ApplianceManagerStatus {
  private final ApplianceManagerId mManagerId;

  public ApplianceManagerStatus(final ApplianceManagerId managerId) {
    mManagerId = managerId;
  }

  public ApplianceManagerId getManagerId() {
    return mManagerId;
  }
}
