package org.kiji.appliance.ipc;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.avro.AvroRemoteException;

import org.kiji.appliance.ApplianceManager;
import org.kiji.appliance.avro.AvroApplianceConfiguration;
import org.kiji.appliance.avro.AvroApplianceId;
import org.kiji.appliance.avro.AvroApplianceInstanceId;
import org.kiji.appliance.avro.AvroApplianceInstanceStatus;
import org.kiji.appliance.avro.AvroApplianceManager;
import org.kiji.appliance.avro.AvroApplianceManagerStatus;
import org.kiji.appliance.avro.AvroApplianceStatus;
import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceId;

public class AvroApplianceManagerImpl implements AvroApplianceManager {
  private final ApplianceManager mApplianceManager;

  public AvroApplianceManagerImpl(final ApplianceManager applianceManager) {
    mApplianceManager = applianceManager;
  }

  @Override
  public AvroApplianceStatus deploy(final AvroApplianceConfiguration configuration) throws AvroRemoteException {
    try {
      return mApplianceManager.deploy(ApplianceConfiguration.fromAvro(configuration)).toAvro();
    } catch (IOException ioe) {
      throw new AvroRemoteException(ioe);
    }
  }

  @Override
  public AvroApplianceStatus undeployAppliance(final AvroApplianceId id) throws AvroRemoteException {
    try {
      return mApplianceManager.undeployAppliance(ApplianceId.fromAvro(id)).toAvro();
    } catch (IOException ioe) {
      throw new AvroRemoteException(ioe);
    }
  }

  @Override
  public AvroApplianceInstanceStatus undeployApplianceInstance(final AvroApplianceInstanceId id) throws AvroRemoteException {
    try {
      return mApplianceManager.undeployApplianceInstance(ApplianceInstanceId.fromAvro(id)).toAvro();
    } catch (IOException ioe) {
      throw new AvroRemoteException(ioe);
    }
  }

  @Override
  public AvroApplianceManagerStatus getStatus() throws AvroRemoteException {
    return null;
  }

  @Override
  public AvroApplianceStatus getApplianceStatus(final AvroApplianceId id) throws AvroRemoteException {
    return null;
  }

  @Override
  public AvroApplianceInstanceStatus getApplianceInstanceStatus(final AvroApplianceInstanceId id) throws AvroRemoteException {
    return null;
  }

  @Override
  public List<AvroApplianceId> listAppliances() throws AvroRemoteException {
    try {
      return Lists.transform(
          mApplianceManager.listAppliances(),
          new Function<ApplianceId, AvroApplianceId>() {
            @Nullable
            @Override
            public AvroApplianceId apply(@Nullable final ApplianceId input) {
              return input.toAvro();
            }
          }
      );
    } catch (IOException ioe) {
      throw new AvroRemoteException(ioe);
    }
  }

  @Override
  public List<AvroApplianceInstanceId> listApplianceInstances(final AvroApplianceId id) throws AvroRemoteException {
    try {
      return Lists.transform(
          mApplianceManager.listApplianceInstances(ApplianceId.fromAvro(id)),
          new Function<ApplianceInstanceId, AvroApplianceInstanceId>() {
            @Nullable
            @Override
            public AvroApplianceInstanceId apply(@Nullable final ApplianceInstanceId input) {
              return input.toAvro();
            }
          }
      );
    } catch (IOException ioe) {
      throw new AvroRemoteException(ioe);
    }
  }
}
