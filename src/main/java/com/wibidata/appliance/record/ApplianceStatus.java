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

package com.wibidata.appliance.record;

import com.wibidata.appliance.avro.AvroApplianceStatus;

public class ApplianceStatus {
  private final ApplianceId mApplianceId;
  private int mInstanceCount;
  private String mName;

  public ApplianceStatus(final ApplianceId applianceId) {
    mApplianceId = applianceId;
  }

  public static ApplianceStatus fromAvro(final AvroApplianceStatus status) {
    return null;
  }

  public ApplianceId getApplianceId() {
    return mApplianceId;
  }

  public AvroApplianceStatus toAvro() {
    return null;
  }

  public int getInstanceCount() {
    return mInstanceCount;
  }

  public String getName() {
    return mName;
  }
}
