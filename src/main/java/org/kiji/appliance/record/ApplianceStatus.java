package org.kiji.appliance.record;

import org.kiji.appliance.avro.AvroApplianceStatus;

public class ApplianceStatus {
  private final ApplianceId mApplianceId;
  private int mInstanceCount;
  private String mName;

  public ApplianceStatus(final ApplianceId applianceId) {
    mApplianceId = applianceId;
  }

  public ApplianceId getApplianceId() {
    return mApplianceId;
  }

  public AvroApplianceStatus toAvro() {
    return null;
  }

  public static ApplianceStatus fromAvro(final AvroApplianceStatus status) {
    return null;
  }

  public int getInstanceCount() {
    return mInstanceCount;
  }

  public String getName() {
    return mName;
  }
}
