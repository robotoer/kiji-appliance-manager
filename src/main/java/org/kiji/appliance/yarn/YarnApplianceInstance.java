package org.kiji.appliance.yarn;

import org.kiji.appliance.ApplianceInstance;
import org.kiji.appliance.record.ApplianceId;
import org.kiji.appliance.record.ApplianceInstanceConfiguration;
import org.kiji.appliance.record.ApplianceInstanceId;
import org.kiji.appliance.record.ApplianceInstanceStatus;

/**
 */
public abstract class YarnApplianceInstance implements ApplianceInstance {
  private final ApplianceInstanceId mInstanceId;
  private final ApplianceId mApplianceId;
  private final ApplianceInstanceConfiguration mConfiguration;

  protected YarnApplianceInstance(
      final ApplianceInstanceId instanceId,
      final ApplianceId applianceId,
      final ApplianceInstanceConfiguration configuration
  ) {
    mInstanceId = instanceId;
    mApplianceId = applianceId;
    mConfiguration = configuration;
  }

  @Override
  public ApplianceInstanceId getInstanceId() {
    return mInstanceId;
  }

  @Override
  public ApplianceId getApplianceId() {
    return mApplianceId;
  }

  @Override
  public ApplianceInstanceConfiguration getConfiguration() {
    return mConfiguration;
  }

  public static class ShellYarnApplicationInstance extends YarnApplianceInstance {
    public static final String GET_STATUS_UNSUPPORTED_MSG =
        "ShellYarnApplicationInstance can't monitor itself currently.";
    public static final String STOP_UNSUPPORTED_MSG =
        "ShellYarnApplicationInstance can't stop itself currently.";
    public static final String START_UNSUPPORTED_MSG =
        "ShellYarnApplicationInstance can't start itself currently.";

    public ShellYarnApplicationInstance(
        final ApplianceInstanceId instanceId,
        final ApplianceId applianceId,
        final ApplianceInstanceConfiguration configuration
    ) {
      super(instanceId, applianceId, configuration);
    }

    @Override
    public ApplianceInstanceStatus getStatus() {
      // TODO: Implement me! This should use YARN's infrastructure to get the container's status.
      throw new UnsupportedOperationException(GET_STATUS_UNSUPPORTED_MSG);
    }

    @Override
    public ApplianceInstanceStatus start() {
      // TODO: Implement me! This should make a request for a resource container.
      throw new UnsupportedOperationException(START_UNSUPPORTED_MSG);
    }

    @Override
    public ApplianceInstanceStatus stop() {
      // TODO: Implement me! This should use YARN's infrastructure to kill the container.
      throw new UnsupportedOperationException(STOP_UNSUPPORTED_MSG);
    }

    @Override
    public ApplianceInstanceStatus restart() {
      stop();
      return start();
    }
  }
}
