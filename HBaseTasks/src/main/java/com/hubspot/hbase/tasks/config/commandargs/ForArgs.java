package com.hubspot.hbase.tasks.config.commandargs;


public class ForArgs {
  private ForArgs() {}

  public static ForArg forArg(final HBaseTaskOption option) {
    return new ForArgImpl(option);
  }
}
