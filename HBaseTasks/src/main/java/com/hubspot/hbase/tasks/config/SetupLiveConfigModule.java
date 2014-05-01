package com.hubspot.hbase.tasks.config;

import com.google.inject.AbstractModule;
import com.hubspot.liveconfig.LiveConfig;
import com.hubspot.liveconfig.LiveConfigModule;

public class SetupLiveConfigModule extends AbstractModule {
  @Override
  protected void configure() {
    LiveConfig liveConfig = LiveConfig.builder()
            .usingEnvironmentVariables()
            .usingSystemProperties()
            .usingPropertiesFile("hbasetasks.properties")
            .build();

    install(new LiveConfigModule(liveConfig));

    bind(LiveConfig.class).toInstance(liveConfig);
  }
}
