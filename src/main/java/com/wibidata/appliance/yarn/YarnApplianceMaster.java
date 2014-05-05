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

import java.net.InetAddress;
import java.util.List;

import com.google.common.base.Objects;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wibidata.appliance.ApplianceManager;
import com.wibidata.appliance.avro.AvroApplianceManager;
import com.wibidata.appliance.avro.AvroApplianceManagerConfiguration;
import com.wibidata.appliance.ipc.AvroApplianceManagerImpl;
import com.wibidata.appliance.record.ApplianceConfiguration;
import com.wibidata.appliance.record.ApplianceId;
import com.wibidata.appliance.record.ApplianceInstanceId;
import com.wibidata.appliance.record.ApplianceInstanceStatus;
import com.wibidata.appliance.record.ApplianceManagerConfiguration;
import com.wibidata.appliance.record.ApplianceManagerId;
import com.wibidata.appliance.record.ApplianceManagerStatus;
import com.wibidata.appliance.record.ApplianceStatus;
import com.wibidata.appliance.util.AvroUtils;

public class YarnApplianceMaster implements ApplianceManager {
  // Exception messages.
  public static final String MANAGER_NOT_RUNNING_MSG = "";
  public static final String NONEXISTENT_APPLIANCE_ID_MSG = "";
  public static final String NONEXISTENT_APPLIANCE_INSTANCE_ID_MSG = "";
  public static final String DUPLICATE_APPLIANCE_NAME_MSG = "";

//  public static final String YARN_APPLIANCE_MASTER_JAVA_FLAGS = "-Xmx256M -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1337";
  public static final String YARN_APPLIANCE_MASTER_JAVA_FLAGS = "-Xmx256M";

  private static final Logger LOG = LoggerFactory.getLogger(YarnApplianceMaster.class);

  public static final String BASE_APPLIANCE_DISCOVERY_PATH = "/org/kiji/appliances";
  public static final int YARN_HEARTBEAT_INTERVAL_MS = 500;
  public static final String CURATOR_SERVICE_NAME = "appliance-master";
  public static final RetryPolicy CURATOR_RETRY_POLICY = new ExponentialBackoffRetry(1000, 3);

  // Yarn connections.
  private final AMRMClientAsync<AMRMClient.ContainerRequest> mResourceManagerClient;
  private final NMClient mNodeManagerClient;

  // Curator connections.
  private final CuratorFramework mCuratorClient;
  private final InstanceSerializer<ApplianceMasterDetails> mJsonSerializer;
  private final ServiceInstance<ApplianceMasterDetails> mThisInstance;

  // Avro RPC server.
  private final HttpServer mHttpServer;

  private final ApplianceManagerConfiguration mMasterConfiguration;
  private final String mMasterAddress;
  // TODO: Investigate using https://github.com/flurry/avro-mobile/blob/master/avro-java-server/src/com/flurry/avroserver/AvroServer.java instead.

  public YarnApplianceMaster(
      final String masterAddress,
      final ApplianceManagerConfiguration masterConfiguration,
      final YarnConfiguration yarnConf
  ) throws Exception {
    mMasterAddress = masterAddress;
    mMasterConfiguration = masterConfiguration;
    // Setup Yarn.
    {
      // Initialize ResourceManager and NodeManager. AMRMClientAsync is used here because it runs a
      // heartbeat thread.
      mResourceManagerClient = AMRMClientAsync.createAMRMClientAsync(YARN_HEARTBEAT_INTERVAL_MS, null);
      mResourceManagerClient.init(yarnConf);
      mResourceManagerClient.start();

      // TODO: Should these be closed?
      mNodeManagerClient = NMClient.createNMClient();
      mNodeManagerClient.init(yarnConf);
      mNodeManagerClient.start();
    }

    // Setup Avro RPC.
    mHttpServer = new HttpServer(
        new SpecificResponder(AvroApplianceManager.class, new AvroApplianceManagerImpl(this)),
        mMasterAddress,
        mMasterConfiguration.getPort()
    );
    System.out.println(String.format("Setup Avro HTTP server on %s:%s", mMasterAddress, masterConfiguration.getPort()));

    // Setup ServiceProvider.
    {
      // Setup a zookeeper connection.
      mCuratorClient = CuratorFrameworkFactory.newClient(masterConfiguration.getCuratorAddress(), CURATOR_RETRY_POLICY);
      mCuratorClient.start();

      final ApplianceMasterDetails masterDetails =
          new ApplianceMasterDetails(masterAddress, masterConfiguration.getPort());
      mThisInstance = ServiceInstance
          .<ApplianceMasterDetails>builder()
          .id(masterConfiguration.getName())
          .name(CURATOR_SERVICE_NAME)
          .address(masterAddress)
          .port(masterConfiguration.getPort())
          .payload(masterDetails)
          .build();

      mJsonSerializer =
          new JsonInstanceSerializer<ApplianceMasterDetails>(ApplianceMasterDetails.class);
    }
  }

  private ServiceDiscovery<ApplianceMasterDetails> getServiceDiscovery() {
    return ServiceDiscoveryBuilder
        .builder(ApplianceMasterDetails.class)
        .client(mCuratorClient)
        .basePath(BASE_APPLIANCE_DISCOVERY_PATH)
        .serializer(mJsonSerializer)
        .thisInstance(mThisInstance)
        .build();
  }

  public void start() throws Exception {
    // Register with ResourceManager.
    // TODO: Are these supposed to not be blank values?
    System.out.println("Registering YarnApplianceMaster...");
    mResourceManagerClient.registerApplicationMaster(mMasterAddress, mMasterConfiguration.getPort(), "");
    System.out.println("Registered YarnApplianceMaster...");

    // Start RPC server.
    mHttpServer.start();

    // Register with Curator's service discovery mechanism.
    final ServiceDiscovery<ApplianceMasterDetails> serviceDiscovery = getServiceDiscovery();
    serviceDiscovery.start();
    try {
      // Does this actually register the application master?
      serviceDiscovery.registerService(mThisInstance);
//
//      final Collection<String> names = serviceDiscovery.queryForNames();
//
//      names.size();
    } finally {
      serviceDiscovery.close();
    }
  }

  public void join() throws Exception {
    mHttpServer.join();
  }

  public void stop() throws Exception {
    // Unregister with Curator's service discovery mechanism.
    final ServiceDiscovery<ApplianceMasterDetails> serviceDiscovery = getServiceDiscovery();
    try {
      serviceDiscovery.start();
      serviceDiscovery.unregisterService(mThisInstance);
    } finally {
      serviceDiscovery.close();
    }
    mCuratorClient.close();

    // Stop Avro RPC.
    mHttpServer.close();

    // Unregister with ResourceManager.
    System.out.println("Unregistering YarnApplianceMaster...");
    mResourceManagerClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
  }

  @Override
  public ApplianceManagerId getId() {
    return null;
  }

  @Override
  public ApplianceStatus deploy(
      final ApplianceConfiguration configuration
  ) {
    // TODO: Implement this!
    // Allocate new resource containers to execute the service.
    System.out.println("Received a deploy call!");
    return null;
  }

  @Override
  public ApplianceStatus undeployAppliance(final ApplianceId id) {
    // TODO: Implement this!
    // Get a handle to the required resource containers to kill their corresponding service.
    System.out.println("Received an undeploy call!");
    return null;
  }

  @Override
  public ApplianceInstanceStatus undeployApplianceInstance(final ApplianceInstanceId id) {
    // TODO: Implement this!
    // Get a handle to the required resource containers to kill their corresponding service instance.
    System.out.println("Received an undeploy call!");
    return null;
  }

  @Override
  public ApplianceManagerStatus getStatus() {
    return null;
  }

  @Override
  public ApplianceStatus getApplianceStatus(final ApplianceId id) {
    return null;
  }

  @Override
  public ApplianceInstanceStatus getApplianceInstanceStatus(final ApplianceInstanceId id) {
    return null;
  }

  @Override
  public List<ApplianceInstanceId> listApplianceInstances(final ApplianceId appliance) {
    // TODO: Implement this!
    // Dump:
    //  - Services that the ApplianceManager is aware of.
    //  - Actual getStatus of the corresponding resource containers.
    System.out.println("Received a list call!");
    return null;
  }

  @Override
  public List<ApplianceId> listAppliances() {
    // TODO: Implement this!
    // Dump:
    //  - Instances that the Service is composed of.
    //  - Actual getStatus of the corresponding service instances.
    System.out.println("Received a list call!");
    return null;
  }

  // ApplicationMaster logic
  public static void main(final String[] args) throws Exception {
    final YarnConfiguration yarnConf = new YarnConfiguration();
    final String masterAddress = InetAddress.getLocalHost().getHostName();
//    final String masterAddress = NetUtils.getHostname();

    // Parse cli arguments.
    final String rawManagerConfiguration = args[0];
    final ApplianceManagerConfiguration managerConfiguration = ApplianceManagerConfiguration.fromAvro(
        AvroUtils.<AvroApplianceManagerConfiguration>fromAvroJsonString(
            rawManagerConfiguration,
            AvroApplianceManagerConfiguration.getClassSchema()
        )
    );

    final YarnApplianceMaster applianceMaster = new YarnApplianceMaster(
        masterAddress,
        managerConfiguration,
        yarnConf
    );
    LOG.info("Starting {}...", applianceMaster.toString());

    System.out.println(String.format("Starting ApplianceMaster: %s", applianceMaster.toString()));
    applianceMaster.start();
    System.out.println("Started ApplianceMaster.");
    try {
      // TODO: Will this actually throw an InterruptedException?
      applianceMaster.join();
    } finally {
      System.out.println("Stopping ApplianceMaster...");
      applianceMaster.stop();
      System.out.println("Stopped ApplianceMaster.");
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("mResourceManagerClient", mResourceManagerClient)
        .add("mNodeManagerClient", mNodeManagerClient)
        .add("mCuratorClient", mCuratorClient)
        .add("mJsonSerializer", mJsonSerializer)
        .add("mThisInstance", mThisInstance)
        .toString();
  }

  public static String prepareArgs(
      final String masterName,
      final int masterPort,
      final String curatorAddress
  ) {
    return String.format("%s %d %s", masterName, masterPort, curatorAddress);
  }

  public String getMasterAddress() {
    return mMasterAddress;
  }

  public ApplianceManagerConfiguration getMasterConfiguration() {
    return mMasterConfiguration;
  }

  public static class ApplianceMasterDetails {
    private final String mMasterAddress;
    private final int mMasterPort;

    public ApplianceMasterDetails(final String masterAddress, final int masterPort) {
      mMasterAddress = masterAddress;
      mMasterPort = masterPort;
    }

    public String getMasterAddress() {
      return mMasterAddress;
    }

    public int getMasterPort() {
      return mMasterPort;
    }
  }
}
