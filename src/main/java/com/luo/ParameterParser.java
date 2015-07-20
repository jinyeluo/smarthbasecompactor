/**
 * Copyright(c) 2015 Merkle Inc.  All Rights Reserved.
 * This software is the proprietary information of Merkle Inc.
 */
package com.luo;

import org.apache.commons.cli.*;

import static java.lang.Integer.parseInt;

public class ParameterParser {

    private final int serverConcurrency;
    private final int minFileCount;
    private final int timePeriod;

    public ParameterParser(String[] args) {
        Options options = createOption();
        try {
            CommandLineParser parser = new GnuParser();
            CommandLine commandLine = parser.parse(options, args);

            timePeriod = parseInt(commandLine.getOptionValue("t", "30"));
            minFileCount = parseInt(commandLine.getOptionValue("f", "2"));
            serverConcurrency = parseInt(commandLine.getOptionValue("c", "2"));
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(ParameterParser.class.getName(), options);
            throw new RuntimeException(e);
        }
    }

    public int getServerConcurrency() {
        return serverConcurrency;
    }

    public int getMinFileCount() {
        return minFileCount;
    }

    public int getTimePeriod() {
        return timePeriod;
    }

    private Options createOption() {
        Options options = new Options();
        Option option = new Option("t", "runPeriod", true,
            "# of minutes it should run, 30 for half an hour and -1 for forever");
        option.setArgName("runPeriod");
        option.setRequired(false);
        option.setArgs(1);
        options.addOption(option);

        option = new Option("f", "minFileThreshold", true,
            "any region with >= fileCount will be compact candidate, defaulted to 2");
        option.setArgName("minFileThreshold");
        option.setRequired(false);
        option.setArgs(1);
        options.addOption(option);

        option = new Option("c", "serverConcurrency", true,
            "max # of regions will be compacted at the same time per server, default to 2");
        option.setArgName("minFileThreshold");
        option.setRequired(false);
        option.setArgs(1);
        options.addOption(option);

        return options;
    }

    @Override
    public String toString() {
        return "ParameterParser{"
            + "serverConcurrency=" + serverConcurrency
            + ", minFileCount=" + minFileCount
            + ", timePeriod=" + timePeriod
            + '}';
    }
}
