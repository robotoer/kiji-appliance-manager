package org.kiji.appliance.ipc;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;

import org.kiji.appliance.ApplianceManager;
import org.kiji.appliance.avro.AvroApplianceId;
import org.kiji.appliance.avro.AvroApplianceInstanceId;
import org.kiji.appliance.avro.AvroApplianceManager;
import org.kiji.appliance.record.ApplianceConfiguration;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceId;
import org.kiji.appliance.record.ApplianceInstanceStatus;
import org.kiji.appliance.record.ApplianceManagerId;
import org.kiji.appliance.record.ApplianceManagerStatus;
import org.kiji.appliance.record.ApplianceStatus;

public class AvroApplianceManagerClient implements ApplianceManager, Closeable {
  final private HttpTransceiver mHttpTransceiver;
  private final AvroApplianceManager mProxy;

  public AvroApplianceManagerClient(final URL rpcUrl) throws IOException {
    mHttpTransceiver = new HttpTransceiver(rpcUrl);
    mProxy = SpecificRequestor.getClient(AvroApplianceManager.class, mHttpTransceiver);
  }

  @Override
  public ApplianceManagerId getId() {
    return null;
  }

  @Override
  public ApplianceStatus deploy(final ApplianceConfiguration configuration) throws IOException {
    return ApplianceStatus.fromAvro(mProxy.deploy(configuration.toAvro()));
  }

  @Override
  public ApplianceStatus undeployAppliance(final ApplianceId id) throws IOException {
    return ApplianceStatus.fromAvro(mProxy.undeployAppliance(id.toAvro()));
  }

  @Override
  public ApplianceInstanceStatus undeployApplianceInstance(final ApplianceInstanceId id) throws IOException {
    return ApplianceInstanceStatus.fromAvro(mProxy.undeployApplianceInstance(id.toAvro()));
  }

  @Override
  public ApplianceManagerStatus getStatus() {
    return null;
  }

  @Override
  public ApplianceStatus getApplianceStatus(ApplianceId id) {
    return null;
  }

  @Override
  public ApplianceInstanceStatus getApplianceInstanceStatus(ApplianceInstanceId id) {
    return null;
  }

  @Override
  public List<ApplianceId> listAppliances() throws IOException {
    return Lists.transform(
        mProxy.listAppliances(),
        new Function<AvroApplianceId, ApplianceId>() {
          @Nullable
          @Override
          public ApplianceId apply(@Nullable final AvroApplianceId input) {
            return ApplianceId.fromAvro(input);
          }
        }
    );
  }

  @Override
  public List<ApplianceInstanceId> listApplianceInstances(final ApplianceId appliance) throws IOException {
    return Lists.transform(
        mProxy.listApplianceInstances(appliance.toAvro()),
        new Function<AvroApplianceInstanceId, ApplianceInstanceId>() {
          @Nullable
          @Override
          public ApplianceInstanceId apply(@Nullable final AvroApplianceInstanceId input) {
            return ApplianceInstanceId.fromAvro(input);
          }
        }
    );
  }

  @Override
  public void close() throws IOException {
    mHttpTransceiver.close();
  }
}
