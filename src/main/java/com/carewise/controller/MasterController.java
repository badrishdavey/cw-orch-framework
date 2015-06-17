package com.carewise.controller;

import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by bdavay on 6/17/15.
 */
public class MasterController {

    private Properties conf;

    public static Logger log = Logger.getLogger(MasterController.class);

    private void loadProperties(String file) throws Exception {

        conf = new Properties();
        conf.load(new FileInputStream(file));

    }


    public static void main(String... args) {
        if (args.length != 1) {
            System.out.println();
            System.out.println("Expected parameters: <Client Properties File Path>");
            log.info("Expected parameters: <Client Properties File Path>");
            System.exit(-1);
        }

        MasterController controller = new MasterController();
        try {
            controller.loadProperties(args[0]);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
