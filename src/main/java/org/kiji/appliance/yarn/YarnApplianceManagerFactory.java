package org.kiji.appliance.yarn;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.appliance.ApplianceManager;
import org.kiji.appliance.ApplianceManagerFactory;
import org.kiji.appliance.record.ApplianceManagerConfiguration;
import org.kiji.appliance.record.ApplianceManagerId;
import org.kiji.appliance.record.ApplianceManagerStatus;

public class YarnApplianceManagerFactory implements ApplianceManagerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(YarnApplianceManagerFactory.class);
  public static final String APPLICATION_MASTER_NAME = "service-master-1";
  public static final String DEFAULT_QUEUE = "default";

  private YarnClient mYarnClient;

  public YarnApplianceManagerFactory(final YarnConfiguration yarnConf) {
    // Create yarnClient
    mYarnClient = YarnClient.createYarnClient();
    mYarnClient.init(yarnConf);
    mYarnClient.start();
  }

  public void launchApplicationMaster(
      final String appName,
      final int appMemory,
      final int appCores,
      final int appAdminPort,
      final String curatorAddress
  ) throws IOException, YarnException, InterruptedException {
    // Create application via yarnClient
    final YarnClientApplication app = mYarnClient.createApplication();

    // Populate the submission context.
    final ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    {
      // Set up the container launch context for the application master
      final ContainerLaunchContext appContainerContext = Records.newRecord(ContainerLaunchContext.class);
      {
        final Configuration config = mYarnClient.getConfig();

//        final String loggedCommand = String.format(
////            "$JAVA_HOME/bin/java %s %s 1>%s/stdout 2>%s/stdout",
//            "${JAVA_HOME}/bin/java %s %s",
//            YarnApplianceMaster.YARN_SERVICE_MASTER_JAVA_FLAGS,
//            YarnApplianceMaster.class.getName()//,
////            appCommand,
////            ApplicationConstants.LOG_DIR_EXPANSION_VAR,
////            ApplicationConstants.LOG_DIR_EXPANSION_VAR
//        );
        final String loggedCommand = String.format(
            "${JAVA_HOME}/bin/java %s %s %s",
            YarnApplianceMaster.YARN_SERVICE_MASTER_JAVA_FLAGS,
            YarnApplianceMaster.class.getName(),
            YarnApplianceMaster.prepareArgs(APPLICATION_MASTER_NAME, appAdminPort, curatorAddress)
        );
        LOG.info("Launching service application master with command: {}", loggedCommand);
        appContainerContext.setCommands(Collections.singletonList(loggedCommand));

        // Copy classpath of client.
        final String classpath = System.getProperty("java.class.path");

        final Map<String, LocalResource> localResources = Maps.newHashMap();
        final Map<String, String> masterEnvVars = Maps.newHashMap();
        for (final String classpathEntry : classpath.split(File.pathSeparator)) {
          if (!classpathEntry.isEmpty()) {
            final Path entryPath = new Path(classpathEntry);
            final FileStatus entryFileStatus =
                FileSystem.getLocal(config).getFileStatus(entryPath);
//            LOG.info("Adding {}", entryFileStatus);
            final URL yarnUrlFromPath = ConverterUtils.getYarnUrlFromPath(entryPath);
            localResources.put(
                entryPath.getName(),
                LocalResource.newInstance(
                    yarnUrlFromPath,
                    LocalResourceType.FILE,
                    LocalResourceVisibility.PUBLIC,
                    entryFileStatus.getLen(),
                    entryFileStatus.getModificationTime()
                )
            );
            yarnUrlFromPath.setScheme("file");
            LOG.debug("Adding {}", localResources.get(entryPath.getName()));
            // TODO: Does this help?
            Apps.addToEnvironment(
                masterEnvVars,
                ApplicationConstants.Environment.CLASSPATH.name(),
                classpathEntry.trim()
            );
          } else {
            LOG.warn("Blank classpath entry found!");
          }
        }

        final String[] yarnConfigClasspath = config.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH
        );
        for (String classpathEntry : yarnConfigClasspath) {
          Apps.addToEnvironment(
              masterEnvVars,
              ApplicationConstants.Environment.CLASSPATH.name(),
              classpathEntry.trim()
          );
        }

        Apps.addToEnvironment(
            masterEnvVars,
            ApplicationConstants.Environment.CLASSPATH.name(),
            ApplicationConstants.Environment.PWD.$() + File.separator + "*"
        );

        appContainerContext.setLocalResources(localResources);
        appContainerContext.setEnvironment(masterEnvVars);

        if (UserGroupInformation.isSecurityEnabled()) {
          final Credentials credentials = new Credentials();
          final String tokenRenewer = config.get(YarnConfiguration.RM_PRINCIPAL);
          if (tokenRenewer == null || tokenRenewer.length() == 0) {
            throw new IOException(
                "Can't get Master Kerberos principal for the RM to use as renewer");
          }
          final FileSystem fs = FileSystem.get(config);
          // For now, only getting tokens for the default file-system.
          final Token<?>[] tokens =
              fs.addDelegationTokens(tokenRenewer, credentials);
          if (tokens != null) {
            for (Token<?> token : tokens) {
              LOG.info("Got dt for " + fs.getUri() + "; " + token);
            }
          }
          final DataOutputBuffer dob = new DataOutputBuffer();
          credentials.writeTokenStorageToStream(dob);
          final ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
          appContainerContext.setTokens(fsTokens);
        } else {
          LOG.info("--------> Security is DISABLED");
        }
//        appContainerContext.setTokens(ByteBuffer.wrap(WritableUtils.toByteArray(mYarnClient.getAMRMToken(appContext.getApplicationId()))));
      }

      // Finally, set-up ApplicationSubmissionContext for the application
      appContext.setApplicationName(appName);
      appContext.setAMContainerSpec(appContainerContext);
      appContext.setResource(Resource.newInstance(appMemory, appCores));
      appContext.setQueue(DEFAULT_QUEUE);
    }

    // Submit application
    final ApplicationId appId = appContext.getApplicationId();
    System.out.println("Submitting application " + appId);
    mYarnClient.submitApplication(appContext);

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
  }

  private void setupAppMasterEnv(Map<String, String> appMasterEnv) { }

  @Override
  public ApplianceManager connect(final ApplianceManagerId id) {
    return null;
  }

  @Override
  public ApplianceManagerStatus start(
      final ApplianceManagerConfiguration configuration
  ) throws IOException, InterruptedException, YarnException {
    launchApplicationMaster(
        configuration.getName(),
        configuration.getMemory(),
        configuration.getCores(),
        configuration.getPort(),
        configuration.getCuratorAddress()
    );

//    return new YarnApplianceManager(mYarnConf.get())
    return null;
  }

  @Override
  public ApplianceManagerStatus stop(final ApplianceManagerId id) {
    return null;
  }
}
