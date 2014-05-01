package org.kiji.appliance.tool;

import java.net.URL;
import java.util.List;

import org.kiji.appliance.ipc.AvroApplianceManagerClient;
import org.kiji.appliance.record.ApplianceId;

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
