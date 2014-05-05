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

package com.wibidata.appliance;

import java.io.IOException;
import java.util.List;

import com.wibidata.appliance.record.ApplianceConfiguration;
import com.wibidata.appliance.record.ApplianceId;
import com.wibidata.appliance.record.ApplianceInstanceId;
import com.wibidata.appliance.record.ApplianceInstanceStatus;
import com.wibidata.appliance.record.ApplianceManagerId;
import com.wibidata.appliance.record.ApplianceManagerStatus;
import com.wibidata.appliance.record.ApplianceStatus;

/**
 * The service manager is responsible for coordinating the deployment/undeployment of services.
 */
public interface ApplianceManager {
  ApplianceManagerId getId();

  ApplianceStatus deploy(final ApplianceConfiguration configuration) throws IOException;

  ApplianceStatus undeployAppliance(final ApplianceId id) throws IOException;

  // TODO: Should this exist? Makes the api unsymmetric.
  ApplianceInstanceStatus undeployApplianceInstance(final ApplianceInstanceId id)
      throws IOException;

  ApplianceManagerStatus getStatus();

  ApplianceStatus getApplianceStatus(final ApplianceId id);

  ApplianceInstanceStatus getApplianceInstanceStatus(final ApplianceInstanceId id);

  List<ApplianceId> listAppliances() throws IOException;

  List<ApplianceInstanceId> listApplianceInstances(final ApplianceId appliance) throws IOException;
}
