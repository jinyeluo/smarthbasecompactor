package com.luo;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionPrint extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionPrint.class);

    protected RegionPrint() {
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info(">>>>> enters main with params:" + ArrayUtils.toString(args));
        int exitCode = ToolRunner.run(new RegionPrint(), args);
        LOGGER.info(">>>>> ready to exit ");
        System.exit(exitCode);
    }

    public int run(String[] aArgs) throws Exception {
        ParameterParser parser = new ParameterParser(aArgs);
        LOGGER.info("{}", parser);

        Configuration config = HBaseConfiguration.create(getConf());
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            try (HbaseCompactor hbaseCompactor = new HbaseCompactor(parser.getMinFileCount(),
                parser.getServerConcurrency(), connection.getAdmin())) {
                hbaseCompactor.printOutRegionsPerServer(config);
            }
        }
        return 0;
    }
}

