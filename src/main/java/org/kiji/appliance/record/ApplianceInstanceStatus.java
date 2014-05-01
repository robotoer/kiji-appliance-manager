package org.kiji.appliance.record;

import org.kiji.appliance.avro.AvroApplianceInstanceStatus;

public class ApplianceInstanceStatus {
  private final ApplianceInstanceId mInstanceId;

  public ApplianceInstanceStatus(final ApplianceInstanceId instanceId) {
    mInstanceId = instanceId;
  }

  public ApplianceInstanceId getInstanceId() {
    return mInstanceId;
  }

  public AvroApplianceInstanceStatus toAvro() {
    return null;
  }

  public static ApplianceInstanceStatus fromAvro(final AvroApplianceInstanceStatus status) {
    return null;
  }
}
