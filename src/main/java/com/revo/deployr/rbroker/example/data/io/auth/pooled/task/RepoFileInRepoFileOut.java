/*
 * RepoFileInRepoFileOut.java
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
package com.revo.deployr.rbroker.example.data.io.auth.pooled.task;

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
import org.apache.commons.io.IOUtils;

import org.apache.log4j.Logger;

public class RepoFileInRepoFileOut
                implements RTaskListener, RBrokerListener {

    private static Logger log =
        Logger.getLogger(RepoFileInRepoFileOut.class);
    private static CountDownLatch latch = new CountDownLatch(1);

    /*
     * Hipparcos star dataset URL endpoint.
     */
    private static String HIP_DAT_URL =
        "http://astrostatistics.psu.edu/datasets/HIP_star.dat";

    private RBroker rBroker = null;

    public static void main(String args[]) throws Exception {

        new RepoFileInRepoFileOut();
    }

    public RepoFileInRepoFileOut() {

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
            PooledBrokerConfig brokerConfig =
                new PooledBrokerConfig(endpoint, rAuth,
                    Integer.getInteger("example-max-concurrency"));

            log.info("[ CONFIGURATION  ] Using broker config " +
                                    "[ PooledBrokerConfig ]");

            /*
             * Establish RBroker connection to DeployR server.
             */
            rBroker =
                RBrokerFactory.pooledTaskBroker(brokerConfig);

            /*
             * Register asynchronous listeners for task and
             * broker runtime events.
             */
            rBroker.addTaskListener(this);
            rBroker.addBrokerListener(this);

            log.info("[   CONNECTION   ] Established authenticated " +
                    "pooled broker [ RBroker ].");

            /*
             * Create the PooledTaskOptions object·
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
            PooledTaskOptions options = 
                                    new PooledTaskOptions();

            /* 
             * Preload from the DeployR repository the following
             * data input file:
             * /testuser/example-data-io/hipStar.dat
             */
            TaskPreloadOptions preloadWorkspace =
                                new TaskPreloadOptions();
            preloadWorkspace.filename = "hipStar.rData";
            preloadWorkspace.directory = "example-data-io";
            preloadWorkspace.author = "testuser";
            options.preloadWorkspace = preloadWorkspace;

            log.info("[   TASK INPUT   ] Repository binary file input " +
                "set on task, [ PooledTaskOptions.preloadWorkspace ].");

            /* 
             * Request repository storage of hip.csv file generated
             * by the dataIO.R script on task execution.
             *
             * Note, you can also use storageOptions.objects
             * to store individual objects from the workspace.
             */
            TaskStorageOptions storageOptions =
                                new TaskStorageOptions();
            storageOptions.files = "hip.csv";
            storageOptions.directory = "example-data-io";
            options.storageOptions = storageOptions;

            log.info("[  TASK OPTION   ] Repository storage request " +
                "set on task [ PooledTaskOptions.storageOptions ].");

            /*
             * Declare task for execution on broker based on
             * repository-managed R script:
             * /testuser/example-data-io/dataIO.R
             */
            RTask rTask = RTaskFactory.pooledTask("dataIO.R",
                                                    "example-data-io",
                                                    "testuser",
                                                    null, options);

            rBroker.submit(rTask);

            log.info("[   EXECUTION    ] Pooled task " +
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

        if(rTaskResult.isSuccess()) {

            log.info("[  TASK RESULT   ] Pooled task " +
                        "completed in " + rTaskResult.getTimeOnCall() +
                        "ms [ RTaskResult ].");

            /*
             * Retrieve stored repository-managed file.
             */

            /*
             * Retrieve stored repository-managed file.
             */
            List<URL> repoFiles = rTaskResult.getStoredFiles();
            DeployRUtil.retrieveFileUrls(repoFiles,
                                        "repository file",
                                        null,
                                        log);

            /*
             * Clean up after example completes.
             */
            DeployRUtil.cleanUpAfterRepoStore(rBroker.owner(),
                                              "hip.csv",
                                              "example-data-io",
                                              "testuser", log);

        } else {
            log.info("[  TASK RESULT   ] Pooled task " +
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
