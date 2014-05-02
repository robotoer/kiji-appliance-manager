package org.kiji.appliance.record;

import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;

import org.kiji.appliance.avro.AvroApplianceManagerConfiguration;

public class ApplianceManagerConfiguration {
  private final String mName;
  private final int mMemory;
  private final int mPort;
  private final int mCores;
  private final String mCuratorAddress;
  // These paths should all refer to files on HDFS.
  private final List<Path> mDependencies;

  public ApplianceManagerConfiguration(
      final String name,
      final int memory,
      final int port,
      final int cores,
      final String curatorAddress,
      final List<Path> dependencies) {
    mName = name;
    mMemory = memory;
    mPort = port;
    mCores = cores;
    mCuratorAddress = curatorAddress;
    mDependencies = dependencies;
  }

  public String getName() {
    return mName;
  }

  public int getMemory() {
    return mMemory;
  }

  public int getPort() {
    return mPort;
  }

  public int getCores() {
    return mCores;
  }

  public String getCuratorAddress() {
    return mCuratorAddress;
  }

  public List<Path> getDependencies() {
    return mDependencies;
  }

  public static ApplianceManagerConfiguration fromAvro(
      final AvroApplianceManagerConfiguration configuration
  ) {
    final List<Path> dependencies = Lists.transform(
        configuration.getDependencies(),
        new Function<String, Path>() {
          @Nullable
          @Override
          public Path apply(@Nullable final String input) {
            return new Path(input);
          }
        }
    );
    return new ApplianceManagerConfiguration(
        configuration.getName(),
        configuration.getMemory(),
        configuration.getPort(),
        configuration.getCores(),
        configuration.getCuratorAddress(),
        dependencies
    );
  }
}
