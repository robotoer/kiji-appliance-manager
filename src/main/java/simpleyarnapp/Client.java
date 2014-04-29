package simpleyarnapp;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

  private final YarnClient mYarnClient;

  public Client(final YarnConfiguration configuration) {
    mYarnClient = YarnClient.createYarnClient();
    mYarnClient.init(configuration);
    mYarnClient.start();
  }
//
//  public void run(String[] args) throws Exception {
//    final String command = args[0];
//    final int n = Integer.valueOf(args[1]);
//    final Path jarPath = new Path(args[2]);
//
//    // Create yarnClient
//    YarnConfiguration conf = new YarnConfiguration();
//    YarnClient yarnClient = YarnClient.createYarnClient();
//    yarnClient.init(conf);
//    yarnClient.start();
//
//    // Create application via yarnClient
//    YarnClientApplication app = yarnClient.createApplication();
//
//    // Set up the container launch context for the application master
//    ContainerLaunchContext amContainer =
//        Records.newRecord(ContainerLaunchContext.class);
//    amContainer.setCommands(
//        Collections.singletonList(
//            "$JAVA_HOME/bin/java" +
//            " -Xmx256M" +
//            " simpleyarnapp.ApplicationMaster" +
//            " " + command +
//            " " + String.valueOf(n) +
//            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
//            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
//            )
//        );
//
//    // Setup jar for ApplicationMaster
//    LocalResource appMasterJar = Records.newRecord(LocalResource.class);
//    setupAppMasterJar(jarPath, appMasterJar);
//    amContainer.setLocalResources(
//        Collections.singletonMap("simpleapp.jar", appMasterJar));
//
//    // Setup CLASSPATH for ApplicationMaster
//    Map<String, String> appMasterEnv = new HashMap<String, String>();
//    setupAppMasterEnv(appMasterEnv);
//    amContainer.setEnvironment(appMasterEnv);
//
//    // Set up resource type requirements for ApplicationMaster
//    Resource capability = Records.newRecord(Resource.class);
//    capability.setMemory(256);
//    capability.setVirtualCores(1);
//
//    // Finally, set-up ApplicationSubmissionContext for the application
//    ApplicationSubmissionContext appContext =
//    app.getApplicationSubmissionContext();
//    appContext.setApplicationName("simple-yarn-app"); // application name
//    appContext.setAMContainerSpec(amContainer);
//    appContext.setResource(capability);
//    appContext.setQueue("default"); // queue
//
//    // Submit application
//    ApplicationId appId = appContext.getApplicationId();
//    System.out.println("Submitting application " + appId);
//    yarnClient.submitApplication(appContext);
//
//    ApplicationReport appReport = yarnClient.getApplicationReport(appId);
//    YarnApplicationState appState = appReport.getYarnApplicationState();
//    while (appState != YarnApplicationState.FINISHED &&
//           appState != YarnApplicationState.KILLED &&
//           appState != YarnApplicationState.FAILED) {
//      Thread.sleep(100);
//      appReport = yarnClient.getApplicationReport(appId);
//      appState = appReport.getYarnApplicationState();
//    }
//
//    System.out.println(
//        "Application " + appId + " finished with" +
//    		" state " + appState +
//    		" at " + appReport.getFinishTime());
//
//  }
  
  private void setupAppMasterJar(
      final Path jarPath,
      final LocalResource appMasterJar
  ) throws IOException {
    FileStatus jarStat = FileSystem.get(mYarnClient.getConfig()).getFileStatus(jarPath);
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
    appMasterJar.setSize(jarStat.getLen());
    appMasterJar.setTimestamp(jarStat.getModificationTime());
    appMasterJar.setType(LocalResourceType.FILE);
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
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
        Environment.PWD.$() + File.separator + "*");
  }

  public static void main(String[] args) throws Exception {
    // Create a new ApplianceManagerFactory.
    final Client client = new Client(new YarnConfiguration());

    // Start the ApplianceManager.
    final ApplianceManagerConfiguration managerConfiguration =
        new ApplianceManagerConfiguration("", 0, 0, 0, "");
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
    // TEMPORARY HARDCODED VALUES
    final String command = "/bin/date";
    final int numberOfContainers = 2;
    final Path jarPath = new Path("hdfs://localhost:8020/user/robert/kiji-appliance-manager-0.1.0-SNAPSHOT.jar");

    // Create application via yarnClient
    final YarnClientApplication app = mYarnClient.createApplication();

    // Set up the container launch context for the application master
    final ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    amContainer.setCommands(
        Collections.singletonList(
            "$JAVA_HOME/bin/java" +
                " -Xmx256M" +
                " simpleyarnapp.ApplicationMaster" +
                " " + command +
                " " + String.valueOf(numberOfContainers) +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
        )
    );

    // Setup jar for ApplicationMaster
    final LocalResource appMasterJar = Records.newRecord(LocalResource.class);
    setupAppMasterJar(jarPath, appMasterJar);
    amContainer.setLocalResources(
        Collections.singletonMap("simpleapp.jar", appMasterJar));

    // Setup CLASSPATH for ApplicationMaster
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv);
    amContainer.setEnvironment(appMasterEnv);

    // Set up resource type requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(256);
    capability.setVirtualCores(1);

    // Finally, set-up ApplicationSubmissionContext for the application
    ApplicationSubmissionContext appContext =
        app.getApplicationSubmissionContext();
    appContext.setApplicationName("simple-yarn-app"); // application name
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue("default"); // queue

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
