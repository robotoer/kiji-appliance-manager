package org.kiji.appliance.record;

public class ApplianceConfiguration {
  private final ApplianceInstanceConfiguration mInstanceConfiguration;
  private final int mInstanceCount;

  public ApplianceConfiguration(
      final ApplianceInstanceConfiguration instanceConfiguration,
      final int instanceCount
  ) {
    mInstanceConfiguration = instanceConfiguration;
    mInstanceCount = instanceCount;
  }

  public ApplianceInstanceConfiguration getInstanceConfiguration() {
    return mInstanceConfiguration;
  }

  public int getInstanceCount() {
    return mInstanceCount;
  }
}
