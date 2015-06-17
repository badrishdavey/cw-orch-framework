package com.carewise.oozie.client;

import org.apache.log4j.Logger;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.local.LocalOozie;

import java.io.FileInputStream;
import java.io.File;
import java.util.Properties;
import java.text.MessageFormat;

import org.apache.hadoop.fs.Path;


/**
 * Created by bdavay on 6/16/15.
 */
public class OozieExecutor {
public static Logger log = Logger.getLogger(OozieExecutor.class);

    public static void main(String... args) {
        System.exit(execute(args));
    }

    static int execute(String... args) {
        if (args.length != 2) {
            System.out.println();
            System.out.println("Expected parameters: <WF_APP_HDFS_URI> <WF_PROPERTIES>");
            log.info("Expected parameters: <WF_APP_HDFS_URI> <WF_PROPERTIES>");
            //return -1;
        }
        //String appUri = args[0];
        //String propertiesFile = args[1];

        String appUri = "hdfs://localhost:50070";
        String propertiesFile = "/Users/bdavay/development/cw-orch-framework/src/main/workflow/job.properties";
        if (propertiesFile != null) {
            File file = new File(propertiesFile);
            if (!file.exists()) {
                System.out.println();
                System.out.println("Specified Properties file does not exist: " + propertiesFile);
                return -1;
            }
            if (!file.isFile()) {
                System.out.println();
                System.out.println("Specified Properties file is not a file: " + propertiesFile);
                return -1;
            }
        }

        try {
            // start local Oozie
            //LocalOozie.start();

            // get a OozieClient for local Oozie
            // OozieClient wc = LocalOozie.getClient();

            OozieClient wc = new OozieClient("http://localhost:11000/oozie");

            // create a workflow job configuration and set the workflow application path
            Properties conf = wc.createConfiguration();
            conf.setProperty(OozieClient.APP_PATH, new Path(appUri, "/worflows/oozie-example/sub-workflows/sub-workflow.xml").toString());
            // load additional workflow job parameters from properties file
            if (propertiesFile != null) {
                conf.load(new FileInputStream(propertiesFile));
            }

            // submit and start the workflow job
            String jobId = wc.run(conf);
            Thread.sleep(1000);
            System.out.println("Workflow job submitted");

            // wait until the workflow job finishes printing the status every 10 seconds
            while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
                System.out.println("Workflow job running ...");
                printWorkflowInfo(wc.getJobInfo(jobId));
                Thread.sleep(10 * 1000);
            }

            // print the final status o the workflow job
            System.out.println("Workflow job completed ...");
            printWorkflowInfo(wc.getJobInfo(jobId));

            return (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED) ? 0 : -1;
        } catch (Exception ex) {
            System.out.println();
            System.out.println(ex.getMessage());
            return -1;
        } finally {
            // stop local Oozie
            // LocalOozie.stop();
        }
    }

    private static void printWorkflowInfo(WorkflowJob wf) {
        System.out.println("Application Path   : " + wf.getAppPath());
        System.out.println("Application Name   : " + wf.getAppName());
        System.out.println("Application Status : " + wf.getStatus());
        System.out.println("Application Actions:");
        for (WorkflowAction action : wf.getActions()) {
            System.out.println(MessageFormat.format("   Name: {0} Type: {1} Status: {2}", action.getName(),
                    action.getType(), action.getStatus()));
        }
        System.out.println();
    }


}

