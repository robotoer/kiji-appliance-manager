package com.wibidata.appliance.record;

import com.wibidata.appliance.avro.AvroApplianceConfiguration;

public class ApplianceConfiguration {
  private final ApplianceInstanceConfiguration mInstanceConfiguration;
  private final int mInstanceCount;
  private String mName;

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

  public String getName() {
    return mName;
  }
}
