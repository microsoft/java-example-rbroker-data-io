/*
 * MultipleDataInMultipleDataOut.java
 *
 * Copyright (c) 2010-2016, Microsoft Corporation
 *
 * This program is licensed to you under the terms of Version 2.0 of the
 * Apache License. This program is distributed WITHOUT
 * ANY EXPRESS OR IMPLIED WARRANTY, INCLUDING THOSE OF NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE. Please refer to the
 * Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) for more details.
 *
 */
package com.revo.deployr.rbroker.example.data.io.auth.discrete.task;

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

public class MultipleDataInMultipleDataOut
                implements RTaskListener, RBrokerListener {

    private static Logger log =
        Logger.getLogger(MultipleDataInMultipleDataOut.class);
    private static CountDownLatch latch = new CountDownLatch(1);

    /*
     * Hipparcos star dataset URL endpoint.
     */
    private static String HIP_DAT_URL =
        "http://astrostatistics.psu.edu/datasets/HIP_star.dat";

    public static void main(String args[]) throws Exception {

        new MultipleDataInMultipleDataOut();
    }

    public MultipleDataInMultipleDataOut() {

        RBroker rBroker = null;

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
            DiscreteBrokerConfig brokerConfig =
                new DiscreteBrokerConfig(endpoint, rAuth,
                    Integer.getInteger("example-max-concurrency"));

            log.info("[ CONFIGURATION  ] Using broker config " +
                                    "[ DiscreteBrokerConfig ]");

            /*
             * Establish RBroker connection to DeployR server.
             */
            rBroker =
                RBrokerFactory.discreteTaskBroker(brokerConfig);

            /*
             * Register asynchronous listeners for task and
             * broker runtime events.
             */
            rBroker.addTaskListener(this);
            rBroker.addBrokerListener(this);

            log.info("[   CONNECTION   ] Established authenticated " +
                    "discrete broker [ RBroker ].");

            /*
             * Create the DiscreteTaskOptions object·
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
            DiscreteTaskOptions options = 
                                    new DiscreteTaskOptions();

            /*
             * MultipleDataInMultipleDataOut Example Note:
             * 
             * The inputs sent on this example are contrived
             * and superfluous as the hipStar.rData binary R
             * object input and the hipStarUrl input perform
             * the exact same purpose...to load the Hip STAR
             * dataset into the workspace ahead of execution.
             * 
             * The example is provided to simply demonstrate
             * the mechanism of specifying multiple inputs.
             */

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
                "set on task, [ DiscreteTaskOptions.preloadWorkspace ].");

            /* 
             * Load an R object literal "hipStarUrl" into the
             * workspace prior to task execution.
             *
             * The R script checks for the existence of "hipStarUrl"
             * in the workspace and if present uses the URL path
             * to load the Hipparcos star dataset from the DAT file
             * at that location.
             */
            RData hipStarUrl =
                RDataFactory.createString("hipStarUrl", HIP_DAT_URL);
            List<RData> rinputs = Arrays.asList(hipStarUrl);
            options.rinputs = rinputs;

            log.info("[   TASK INPUT   ] External data source input " +
                "set on task, [ DiscreteTaskOptions.rinputs ].");

            /*
             * Request the retrieval of the "hip" data.frame and
             * two vector objects from the workspace following the
             * task execution. The corresponding R objects are named
             * as follows:
             * 'hip', hipDim', 'hipNames'.
             */
            options.routputs =
                Arrays.asList("hip", "hipDim", "hipNames");

            log.info("[  TASK OPTION   ] DeployR-encoded R object request " +
                "set on task [ DiscreteTaskOptions.routputs ].");

            /*
             * Declare task for execution on broker based on
             * repository-managed R script:
             * /testuser/example-data-io/dataIO.R
             */
            RTask rTask = RTaskFactory.discreteTask("dataIO.R",
                                                    "example-data-io",
                                                    "testuser",
                                                    null, options);

            rBroker.submit(rTask);

            log.info("[   EXECUTION    ] Discrete task " +
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
            
            log.info("[  TASK RESULT   ] Discrete task " +
                        "completed in " + rTaskResult.getTimeOnCall() +
                        "ms [ RTaskResult ].");

            String console = rTaskResult.getGeneratedConsole();
            log.info("[  TASK OUTPUT   ] Retrieved R console " +
                "output [ String ].");

            /*
             * Retrieve the requested R object data encodings from
             * the workspace follwing the task execution. 
             *
             * See the R Object Data Decoding chapter in the
             * Client Library Tutorial on the DeployR website for
             * further details.
             */
            List<RData> objects = rTaskResult.getGeneratedObjects();
            DeployRUtil.retrieveWorkspaceObjects(objects, log);

            /*
             * Retrieve the working directory files (artifact)
             * that were generated by the execution.
             */
            List<URL> wdFiles = rTaskResult.getGeneratedFiles();
            DeployRUtil.retrieveFileUrls(wdFiles,
                                        "working directory file",
                                        null,
                                        log);

            /*
             * Retrieve R graphics device plots (results) called
             * unnamedplot*.png that were generated by the execution.
             */
            List<URL> results = rTaskResult.getGeneratedPlots();
            DeployRUtil.retrieveFileUrls(results,
                                        "graphics device plot",
                                        ".png",
                                        log);

        } else {
            log.info("[  TASK RESULT   ] Discrete task " +
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
