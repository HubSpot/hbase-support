package com.hubspot.hbase.tasks.config.commandargs;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;

import java.util.ListIterator;

public class IgnoreOptionParser extends GnuParser {
  @Override
  protected void processOption(final String arg, final ListIterator iter) throws ParseException {
    final boolean hasOption = getOptions().hasOption(arg);
    if (hasOption) {
      super.processOption(arg, iter);
    }
  }
}
