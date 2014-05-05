package org.kiji.appliance;

import java.util.UUID;

import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.After;
import org.junit.Before;

public class YarnClientTest {
  private MiniYARNCluster mCluster = null;

  protected MiniYARNCluster getCluster() {
    return mCluster;
  }

  protected YarnConfiguration getConfig() {
    return new YarnConfiguration(mCluster.getConfig());
  }

  @Before
  public void startMiniYarnCluster() throws Exception {
    final YarnConfiguration configuration = new YarnConfiguration();
    configuration.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
    configuration.setClass(
        YarnConfiguration.RM_SCHEDULER,
        FifoScheduler.class,
        ResourceScheduler.class
    );

    // This currently fails if multiple are instantiated.
//    Preconditions.checkState(null != mCluster, "Cluster was already started!");
    mCluster = new MiniYARNCluster(
        String.format("test-yarn-cluster-%s", UUID.randomUUID().toString()),
        2,
        1,
        1
    );
    mCluster.init(configuration);
    mCluster.start();
  }

  @After
  public void stopMiniYarnCluster() {
    mCluster.stop();
  }
}
