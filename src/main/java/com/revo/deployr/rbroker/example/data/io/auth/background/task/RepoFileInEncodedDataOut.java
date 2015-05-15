/*
 * RepoFileInEncodedDataOut.java
 *
 * Copyright (C) 2010-2015 by Revolution Analytics Inc.
 *
 * This program is licensed to you under the terms of Version 2.0 of the
 * Apache License. This program is distributed WITHOUT
 * ANY EXPRESS OR IMPLIED WARRANTY, INCLUDING THOSE OF NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE. Please refer to the
 * Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) for more details.
 *
 */
package com.revo.deployr.rbroker.example.data.io.auth.background.task;

import com.revo.deployr.client.*;
import com.revo.deployr.client.auth.*;
import com.revo.deployr.client.auth.basic.*;
import com.revo.deployr.client.data.*;
import com.revo.deployr.client.factory.*;
import com.revo.deployr.client.params.*;
import com.revo.deployr.client.broker.*;
import com.revo.deployr.client.broker.config.*;
import com.revo.deployr.client.broker.task.*;
import com.revo.deployr.client.broker.options.*;
import com.revo.deployr.rbroker.example.util.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

public class RepoFileInEncodedDataOut
                implements RTaskListener, RBrokerListener {

    private static Logger log =
        Logger.getLogger(RepoFileInEncodedDataOut.class);
    private static CountDownLatch latch = new CountDownLatch(1);
    private RBroker rBroker = null;

    /*
     * Hipparcos star dataset URL endpoint.
     */
    private static String HIP_DAT_URL =
        "http://astrostatistics.psu.edu/datasets/HIP_star.dat";

    public static void main(String args[]) throws Exception {

        new RepoFileInEncodedDataOut();
    }

    public RepoFileInEncodedDataOut() {

        try {

            /*
             * Determine DeployR server endpoint.
             */
            String endpoint = System.getProperty("endpoint");
            log.info("[ CONFIGURATION  ] Using endpoint [ " +
                                        endpoint + " ]");

            /*
             * Build configuration to initialize RBroker instance.
             */
            RAuthentication rAuth = new RBasicAuthentication(
                                        System.getProperty("username"),
                                        System.getProperty("password"));
            BackgroundBrokerConfig brokerConfig =
                        new BackgroundBrokerConfig(endpoint, rAuth);

            log.info("[ CONFIGURATION  ] Using broker config " +
                                    "[ BackgroundBrokerConfig ]");

            /*
             * Establish RBroker connection to DeployR server.
             */
            rBroker =
                RBrokerFactory.backgroundTaskBroker(brokerConfig);

            /*
             * Register asynchronous listeners for task and
             * broker runtime events.
             */
            rBroker.addTaskListener(this);
            rBroker.addBrokerListener(this);

            log.info("[   CONNECTION   ] Established authenticated " +
                    "background broker [ RBroker ].");

            /*
             * Create the BackgroundTaskOptions object·
             * to specify data inputs and output to the task.
             *
             * This options object can be used to pass standard
             * execution model parameters on execution calls. All
             * fields are optional.
             *
             * See the Standard Execution Model chapter in the
             * Client Library Tutorial on the DeployR website for
             * further details.
             */
            BackgroundTaskOptions options = 
                                    new BackgroundTaskOptions();

            /* 
             * Preload from the DeployR repository the following
             * binary R object input file:
             * /testuser/example-data-io/hipStar.rData
             */
            TaskPreloadOptions preloadWorkspace =
                                new TaskPreloadOptions();
            preloadWorkspace.filename = "hipStar.rData";
            preloadWorkspace.directory = "example-data-io";
            preloadWorkspace.author = "testuser";
            options.preloadWorkspace = preloadWorkspace;

            log.info("[   TASK INPUT   ] Repository binary file input " +
                "set on task, [ BackgroundTaskOptions.preloadWorkspace ].");

            /*
             * Declare task for execution on broker based on
             * repository-managed R script:
             * /testuser/example-data-io/dataIO.R
             */
            RTask rTask = RTaskFactory.backgroundTask(this.toString(),
                                                    "Data I/O tutorial.",
                                                    "dataIO.R",
                                                    "example-data-io",
                                                    "testuser",
                                                    null, options);

            rBroker.submit(rTask);

            log.info("[   EXECUTION    ] Background task " +
                    "submitted to broker [ RTask ].");

            /*
             * Simple block on main thread of this sample application
             * until example RTask execution completes.
             */
            latch.await();

        } catch(Exception ex) {
            log.warn("Unexpected error: ex=" + ex);
        } finally {
            /*
             * Clean-up after example completes.
             */
            if(rBroker != null) {
                try{
                    rBroker.shutdown();
                } catch(Exception bex) {
                    log.warn("RBroker.shutdown ex=" + bex);
                }
            }
        }            

    }

    /*
     * RBroker asynchronous callback listener on RTask completion.
     */
    public void onTaskCompleted(RTask rTask, RTaskResult rTaskResult) {

        RUser rUser = rBroker.owner();

        if(rTaskResult.isSuccess()) {

            try {

                log.info("[  TASK RESULT   ] Background task " +
                            "queued in " + rTaskResult.getTimeOnCall() +
                            "ms [ RTaskResult ].");

                /*
                 * Retrieve handle to RJob associated with RTask.
                 */
                RJob rJob = rUser.queryJob(rTaskResult.getID());

                /*
                 * Wait for RJob associated with background task 
                 * to complete, or fail.
                 */
                boolean jobIsSuccess =
                    DeployRUtil.verifyJobExitStatus(rJob,
                                                    RJob.COMPLETED);

                if(jobIsSuccess) {

                    log.info("[  TASK RESULT   ] Background task " +
                             "completed in " + rJob.about().timeTotal +
                             "ms [ RJob ]");

                    /*
                     * Retrieve handle to RProject generated by
                     * RJob associated with RTask.
                     */
                    RProject rProject =
                        rUser.getProject(rJob.about().project);

                    /*
                     * Retrieve the requested R object data encodings from
                     * the workspace following the task execution. 
                     *
                     * See the R Object Data Decoding chapter in the
                     * Client Library Tutorial on the DeployR website for
                     * further details.
                     */
                    List<String> objectList =
                        Arrays.asList("hip", "hipDim", "hipNames");
                    List<RData> objects = rProject.getObjects(objectList);
                    DeployRUtil.retrieveWorkspaceObjects(objects, log);

                } else {

                    // Job failed...
                    log.info("[  TASK FAILED   ] Background job " +
                                "failed in " + rTaskResult.getTimeOnCall() +
                                "ms [ RTaskResult ].");
                }

            } catch(Exception tex) {

                log.info("Handling background task result ex=" + tex);

            } finally {
                DeployRUtil.cleanUpAfterJob(rUser, rTaskResult.getID());
            }

        } else {
            log.info("[  TASK RESULT   ] Background task " +
                    "failed, cause " + rTaskResult.getFailure());
        }

        /*
         * Unblock main thread so example application can release 
         * resources before exit.
         */
        latch.countDown();
    }

    /*
     * RBroker asynchronous callback listener on RTask error.
     */
    public void onTaskError(RTask rTask, Throwable throwable) {
        log.info("onTaskError: rTask=" + rTask +
                                ", error=" + throwable);
        /*
         * Unblock main thread so example application can release 
         * resources before exit.
         */
        latch.countDown();
    }

    public void onRuntimeError(Throwable throwable) {
        log.info("onRuntimeError: cause=" + throwable); 
        /*
         * Unblock main thread so example application can release 
         * resources before exit.
         */
        latch.countDown();
    }

    public void onRuntimeStats(RBrokerRuntimeStats stats,
                                        int maxConcurrency) {
        /*
         * Log/profile runtime RBroker stats as desired...
         */
    }

}
