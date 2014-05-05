/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wibidata.appliance.ipc;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.avro.AvroRemoteException;

import com.wibidata.appliance.ApplianceManager;
import com.wibidata.appliance.avro.AvroApplianceConfiguration;
import com.wibidata.appliance.avro.AvroApplianceId;
import com.wibidata.appliance.avro.AvroApplianceInstanceId;
import com.wibidata.appliance.avro.AvroApplianceInstanceStatus;
import com.wibidata.appliance.avro.AvroApplianceManager;
import com.wibidata.appliance.avro.AvroApplianceManagerStatus;
import com.wibidata.appliance.avro.AvroApplianceStatus;
import com.wibidata.appliance.record.ApplianceConfiguration;
import com.wibidata.appliance.record.ApplianceId;
import com.wibidata.appliance.record.ApplianceInstanceId;

public class AvroApplianceManagerImpl implements AvroApplianceManager {
  private final ApplianceManager mApplianceManager;

  public AvroApplianceManagerImpl(final ApplianceManager applianceManager) {
    mApplianceManager = applianceManager;
  }

  @Override
  public AvroApplianceStatus deploy(
      final AvroApplianceConfiguration configuration
  ) throws AvroRemoteException {
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
