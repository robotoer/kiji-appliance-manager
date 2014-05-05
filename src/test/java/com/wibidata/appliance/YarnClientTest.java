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

import java.util.UUID;

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
