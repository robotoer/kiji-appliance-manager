package org.kiji.appliance.record;

import org.kiji.appliance.avro.AvroApplianceConfiguration;

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

  public static ApplianceConfiguration fromAvro(final AvroApplianceConfiguration configuration) {
    return null;
  }

  public AvroApplianceConfiguration toAvro() {
    return null;
  }
}
