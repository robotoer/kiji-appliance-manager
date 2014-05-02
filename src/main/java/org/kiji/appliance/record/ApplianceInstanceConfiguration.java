package org.kiji.appliance.record;

public class ApplianceInstanceConfiguration {
  private final String mType;
  private final String mCommand;

  public ApplianceInstanceConfiguration(
      final String type,
      final String command
  ) {
    mType = type;
    mCommand = command;
  }

  public String getType() {
    return mType;
  }

  public String getCommand() {
    return mCommand;
  }
}
