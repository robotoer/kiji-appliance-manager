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

package com.wibidata.appliance.tool;

import java.net.URL;
import java.util.List;

import com.wibidata.appliance.ipc.AvroApplianceManagerClient;
import com.wibidata.appliance.record.ApplianceId;

/**
 * WILL BE REMOVED IN THE FUTURE.
 */
public class ApplianceManagerClientTool {
  public static void main(final String[] args) throws Exception {
    // Setup client.
    System.out.println("Starting client...");
    final URL rpcUrl = new URL("http", "localhost", 58738, "/");
    final AvroApplianceManagerClient client = new AvroApplianceManagerClient(rpcUrl);
    System.out.println(String.format("Started client connected to: %s", rpcUrl.toString()));

    try {
      // List the current appliances.
      final List<ApplianceId> applianceIds = client.listAppliances();
      for (ApplianceId applianceId : applianceIds) {
        System.out.println(applianceId.toString());
      }
    } finally {
      client.close();
    }
  }
}
