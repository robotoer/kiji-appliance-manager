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

  public static ApplianceConfiguration fromAvro(final AvroApplianceConfiguration configuration) {
    return null;
  }

  public ApplianceInstanceConfiguration getInstanceConfiguration() {
    return mInstanceConfiguration;
  }

  public int getInstanceCount() {
    return mInstanceCount;
  }

  public AvroApplianceConfiguration toAvro() {
    return null;
  }

  public String getName() {
    return mName;
  }
}
