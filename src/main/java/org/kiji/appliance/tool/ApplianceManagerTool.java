package org.kiji.appliance.tool;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.kiji.appliance.record.ApplianceManagerConfiguration;
import org.kiji.appliance.yarn.YarnApplianceManagerFactory;

// TODO: Make this a KijiTool.
public class ApplianceManagerTool {
  public static void main(String[] args) throws Exception {
    final YarnConfiguration conf = new YarnConfiguration();

    // TEMPORARY HARDCODED VALUES
    // Generated using 'mvn dependency:build-classpath -DincludeScope=runtime
    final String runtimeClasspath = "/home/robert/.m2/repository/org/apache/curator/curator-framework/2.4.1/curator-framework-2.4.1.jar:/home/robert/.m2/repository/org/jboss/netty/netty/3.2.2.Final/netty-3.2.2.Final.jar:/home/robert/.m2/repository/com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar:/home/robert/.m2/repository/org/slf4j/slf4j-api/1.7.2/slf4j-api-1.7.2.jar:/home/robert/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:/home/robert/.m2/repository/log4j/log4j/1.2.16/log4j-1.2.16.jar:/home/robert/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.9.13/jackson-core-asl-1.9.13.jar:/home/robert/.m2/repository/com/thoughtworks/paranamer/paranamer/2.3/paranamer-2.3.jar:/home/robert/.m2/repository/org/apache/curator/curator-client/2.4.1/curator-client-2.4.1.jar:/home/robert/.m2/repository/org/apache/curator/curator-recipes/2.4.1/curator-recipes-2.4.1.jar:/home/robert/.m2/repository/org/xerial/snappy/snappy-java/1.0.5/snappy-java-1.0.5.jar:/home/robert/.m2/repository/org/tukaani/xz/1.0/xz-1.0.jar:/home/robert/.m2/repository/org/apache/avro/avro/1.7.5/avro-1.7.5.jar:/home/robert/.m2/repository/org/apache/commons/commons-compress/1.4.1/commons-compress-1.4.1.jar:/home/robert/.m2/repository/org/apache/curator/curator-x-discovery/2.4.1/curator-x-discovery-2.4.1.jar:/home/robert/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.9.13/jackson-mapper-asl-1.9.13.jar:/home/robert/.m2/repository/org/apache/zookeeper/zookeeper/3.4.5/zookeeper-3.4.5.jar:/home/robert/.m2/repository/org/slf4j/slf4j-log4j12/1.7.2/slf4j-log4j12-1.7.2.jar:/home/robert/.m2/repository/jline/jline/0.9.94/jline-0.9.94.jar:/home/robert/.m2/repository/commons-io/commons-io/2.1/commons-io-2.1.jar";
    final String applianceManagerJarPath = "/home/robert/src/kiji-appliance-manager/target/kiji-appliance-manager-0.1.0-SNAPSHOT.jar";
    // TODO: Make this change per launch (perhaps use a timestamp/applicationId).
    final String baseResourcePath = "hdfs://localhost:8020/user/robert/appliances/lib/";

    // Copy the required jars to hdfs. Jars will be placed in hdfs://${USER_HOME}/appliances/lib
    final ImmutableList.Builder<Path> dependencyJarsBuilder = ImmutableList.builder();
    final Path libApplianceManagerJarPath = copyDependency(conf, applianceManagerJarPath, baseResourcePath);
    dependencyJarsBuilder.add(libApplianceManagerJarPath);
    for (final String classpathEntry : runtimeClasspath.split(File.pathSeparator)) {
      final Path libResourcePath = copyDependency(conf, classpathEntry, baseResourcePath);
      dependencyJarsBuilder.add(libResourcePath);
      System.out.println(String.format("Copied %s to %s", classpathEntry, libResourcePath.toString()));
    }

    // Create a new ApplianceManagerFactory.
    final YarnApplianceManagerFactory managerFactory = new YarnApplianceManagerFactory(conf);

    // Start the ApplianceManager.
    final ApplianceManagerConfiguration managerConfiguration = new ApplianceManagerConfiguration(
        "appliance-manager-1",
        256,
        0,
        1,
        "localhost:2181",
        dependencyJarsBuilder.build()
    );
    System.out.println(String.format("Starting application with configuration: %s", managerConfiguration.toString()));
    managerFactory.start(managerConfiguration);
  }

  private static Path copyDependency(final YarnConfiguration conf, final String resourcePath, final String baseResourcePath) throws IOException {
    final Path localApplianceManagerJarPath = new Path("file://" + resourcePath);
    final Path libApplianceManagerJarPath = new Path(baseResourcePath + localApplianceManagerJarPath.getName());
    Preconditions.checkState(
        FileUtil.copy(
            localApplianceManagerJarPath.getFileSystem(conf),
            localApplianceManagerJarPath,
            libApplianceManagerJarPath.getFileSystem(conf),
            libApplianceManagerJarPath,
            false,
            true,
            conf
        ),
        "File copy failed: %s to %s", localApplianceManagerJarPath, libApplianceManagerJarPath
    );
    Preconditions.checkState(
        libApplianceManagerJarPath.getFileSystem(conf).exists(libApplianceManagerJarPath),
        "File copy failed: %s to %s", localApplianceManagerJarPath, libApplianceManagerJarPath
    );
    return libApplianceManagerJarPath;
  }
}
