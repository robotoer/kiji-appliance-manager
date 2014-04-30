package simpleyarnapp;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import org.kiji.appliance.ApplianceManager;
import org.kiji.appliance.ApplianceManagerFactory;
import org.kiji.appliance.record.ApplianceManagerConfiguration;
import org.kiji.appliance.record.ApplianceManagerId;
import org.kiji.appliance.record.ApplianceManagerStatus;


public class Client implements ApplianceManagerFactory, Closeable {
  public static final String CONNECT_IS_NOT_CURRENTLY_SUPPORTED_MSG = "Connect is not currently supported.";
  public static final String STOP_IS_NOT_CURRENTLY_SUPPORTED_MSG = "Stop is not currently supported";
  public static final String DEFAULT_QUEUE = "default";

  private final YarnClient mYarnClient;

  public Client(final YarnConfiguration configuration) {
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

  public static void main(String[] args) throws Exception {
    // TEMPORARY HARDCODED VALUES
    final Path jarPath = new Path("hdfs://localhost:8020/user/robert/kiji-appliance-manager-0.1.0-SNAPSHOT.jar");

    // Create a new ApplianceManagerFactory.
    final Client client = new Client(new YarnConfiguration());

    // Start the ApplianceManager.
    final ApplianceManagerConfiguration managerConfiguration =
        new ApplianceManagerConfiguration("appliance-manager-1", 256, 0, 1, "", Lists.newArrayList(jarPath));
    System.out.println(String.format("Starting application with configuration: %s", managerConfiguration.toString()));
    client.start(managerConfiguration);
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
    amContainer.setCommands(
        Collections.singletonList(
            "$JAVA_HOME/bin/java" +
                " -Xmx256M" +
                " simpleyarnapp.ApplicationMaster" +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
        )
    );

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
    ApplicationId appId = appContext.getApplicationId();
    System.out.println("Submitting application " + appId);
    mYarnClient.submitApplication(appContext);

    ApplicationReport appReport = mYarnClient.getApplicationReport(appId);
    YarnApplicationState appState = appReport.getYarnApplicationState();
    while (
        appState != YarnApplicationState.FINISHED &&
        appState != YarnApplicationState.KILLED &&
        appState != YarnApplicationState.FAILED) {
      Thread.sleep(100);
      appReport = mYarnClient.getApplicationReport(appId);
      appState = appReport.getYarnApplicationState();
    }

    System.out.println(
        "Application " + appId + " finished with" +
            " state " + appState +
            " at " + appReport.getFinishTime());

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
