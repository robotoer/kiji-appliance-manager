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

package com.wibidata.appliance.yarn;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.wibidata.appliance.ApplianceManager;
import com.wibidata.appliance.ApplianceManagerFactory;
import com.wibidata.appliance.record.ApplianceManagerConfiguration;
import com.wibidata.appliance.record.ApplianceManagerId;
import com.wibidata.appliance.record.ApplianceManagerStatus;

public class YarnApplianceManagerFactory implements ApplianceManagerFactory, Closeable {
  public static final String CONNECT_IS_NOT_CURRENTLY_SUPPORTED_MSG = "Connect is not currently supported.";
  public static final String STOP_IS_NOT_CURRENTLY_SUPPORTED_MSG = "Stop is not currently supported";

  public static final String DEFAULT_QUEUE = "default";

  private final YarnClient mYarnClient;

  public YarnApplianceManagerFactory(final YarnConfiguration configuration) {
    mYarnClient = YarnClient.createYarnClient();
    mYarnClient.init(configuration);
    mYarnClient.start();
  }

  /**
   * Creates a local resource definition for a path referring to a file on HDFS.
   *
   * @param resourcePath to a file on HDFS.
   * @return a local resource for an appliance.
   */
  private LocalResource pathToLocalResource(
      final Path resourcePath
  ) throws IOException {
    final FileStatus resourceStat = FileSystem.get(mYarnClient.getConfig()).getFileStatus(resourcePath);
    Preconditions.checkArgument(resourceStat.isFile());

    final LocalResource appMasterJar = Records.newRecord(LocalResource.class);
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(resourcePath));
    appMasterJar.setSize(resourceStat.getLen());
    appMasterJar.setTimestamp(resourceStat.getModificationTime());
    appMasterJar.setType(LocalResourceType.FILE);
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);

    return appMasterJar;
  }

  private void setupAppMasterEnv(
      final Map<String, String> appMasterEnv
  ) {
    for (String c : mYarnClient.getConfig().getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
          c.trim());
    }
    Apps.addToEnvironment(appMasterEnv,
        Environment.CLASSPATH.name(),
        Environment.PWD.$() + File.separator + "*"
    );
  }

  @Override
  public ApplianceManager connect(final ApplianceManagerId id) {
    throw new UnsupportedOperationException(CONNECT_IS_NOT_CURRENTLY_SUPPORTED_MSG);
  }

  @Override
  public ApplianceManagerStatus start(
      final ApplianceManagerConfiguration managerConfiguration
  ) throws IOException, InterruptedException, YarnException {
    // Create application via yarnClient
    final YarnClientApplication app = mYarnClient.createApplication();

    // Set up the container launch context for the application master
    final ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    final String loggedCommand = String.format(
        "${JAVA_HOME}/bin/java %s %s %s 1>%s/stdout 2>%s/stderr",
        YarnApplianceMaster.YARN_APPLIANCE_MASTER_JAVA_FLAGS,
        YarnApplianceMaster.class.getName(),
        YarnApplianceMaster.prepareArgs(
            managerConfiguration.getName(),
            managerConfiguration.getPort(),
            managerConfiguration.getCuratorAddress()
        ),
        ApplicationConstants.LOG_DIR_EXPANSION_VAR,
        ApplicationConstants.LOG_DIR_EXPANSION_VAR
    );
    System.out.println(
        String.format("Launching appliance application master with command: %s", loggedCommand)
    );
    amContainer.setCommands(Collections.singletonList(loggedCommand));

    // Setup jars for ApplicationMaster
    {
      final List<Path> dependencies = managerConfiguration.getDependencies();
      final HashMap<String, LocalResource> localResources = Maps.newHashMap();
      for (Path dependency : dependencies) {
        localResources.put(
            dependency.getName(),
            pathToLocalResource(dependency)
        );
      }
      amContainer.setLocalResources(localResources);
    }

    // Setup CLASSPATH for ApplicationMaster
    final Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv);
    amContainer.setEnvironment(appMasterEnv);

    // Set up resource type requirements for ApplicationMaster
    final Resource capability =
        Resource.newInstance(managerConfiguration.getMemory(), managerConfiguration.getCores());

    // Finally, set-up ApplicationSubmissionContext for the application
    final ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    appContext.setApplicationName(managerConfiguration.getName());
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue(DEFAULT_QUEUE); // queue

    // Submit application
    final ApplicationId appId = appContext.getApplicationId();
    System.out.println("Submitting application " + appId);
    mYarnClient.submitApplication(appContext);

    // Keep track of the running application
    ApplicationReport appReport = mYarnClient.getApplicationReport(appId);
    YarnApplicationState appState = appReport.getYarnApplicationState();
    while (
        appState != YarnApplicationState.FINISHED &&
        appState != YarnApplicationState.KILLED &&
        appState != YarnApplicationState.FAILED
    ) {
      Thread.sleep(100);
      appReport = mYarnClient.getApplicationReport(appId);
      appState = appReport.getYarnApplicationState();
    }

    System.out.println(
        String.format(
            "Application %s finished with state %s at %d",
            appId,
            appState,
            appReport.getFinishTime()
        )
    );

    return null;
  }

  @Override
  public ApplianceManagerStatus stop(final ApplianceManagerId id) {
    throw new UnsupportedOperationException(STOP_IS_NOT_CURRENTLY_SUPPORTED_MSG);
  }

  @Override
  public void close() throws IOException {
    // TODO: What if this was never opened/started?
    mYarnClient.close();
  }
}
