/*
 * DeployRUtil.java
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
package com.revo.deployr.rbroker.example.util;

import com.revo.deployr.client.*;
import com.revo.deployr.client.about.*;
import com.revo.deployr.client.data.*;
import com.revo.deployr.client.factory.RDataFactory;
import com.revo.deployr.client.params.*;
import com.revo.deployr.client.broker.options.*;

import java.io.*;
import java.net.URL;
import java.util.*;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class DeployRUtil {

    /*
     * simulateGeneratedData
     *
     * This method is used to generate sample data within the
     * application. In a real-world application this data may 
     * originate from any number of sources, for example:
     *
     * - Data read from a file
     * - Data read from a database
     * - Data read from an external Web service
     * - Data read from direct user input
     * - Data generated in real time by the application itself
     *
     * This data is encoded using the RDataFactory and then
     * passed as an input the execution.
     *
     */
    public static RData simulateGeneratedData(Logger log) {

        RData df = null;
        try {

            URL url =
                new URL("http://astrostatistics.psu.edu/datasets/HIP_star.dat");
            InputStream is = url.openStream();
            RDataTable table = RDataFactory.createDataTable(is, "\\s+", true, true);
            df = table.asDataFrame("hip");

        } catch(Exception ex) {
            log.warn("Simulate generated data failed, ex=" + ex);
        } finally {
            return df;
        }

    }

    public static void retrieveWorkspaceObjects(List<RData> objects,
                                                        Logger log) {

        try {

            for(RData rData : objects) {
                if(rData instanceof RDataFrame) {
                    log.info("[  TASK OUTPUT   ] Retrieved " +
                        "DeployR-encoded R " +
                        "object output " + rData.getName() +
                        " [ RDataFrame ].");
                    List<RData> hipSubsetVal =
                        ((RDataFrame) rData).getValue();
                    /*
                     * Optionally convert RDataFrame to RTableData
                     * to simplify working with data values within
                     * the object.
                     */
                    try {
                        RDataTable table =
                            RDataFactory.createDataTable(rData);
                    } catch(RDataException dex) {
                       log.warn("Unexpected data.frame to table " +
                                                "error: ex=" + dex);
                    }
                } else
                if(rData instanceof RNumericVector) {
                    log.info("[  TASK OUTPUT   ] Retrieved " +
                        "DeployR-encoded R " +
                        "object output " + rData.getName() +
                        " [ RNumericVector ].");
                    List<Double> hipDimVal =
                        ((RNumericVector) rData).getValue();
                    log.info("[  TASK OUTPUT   ] Retrieved " +
                        "DeployR-encoded R " +
                        "object " + rData.getName() +
                        " value=" + hipDimVal);
                    /*
                     * Optionally convert RDataFrame to RTableData
                     * to simplify working with data values within
                     * the object.
                     */
                    try {
                        RDataTable table =
                            RDataFactory.createDataTable(
                                        (RNumericVector) rData);
                    } catch(RDataException dex) {
                       log.warn("Unexpected num.vector to table " +
                                                "error: ex=" + dex);
                    }
                } else
                if(rData instanceof RStringVector) {
                    log.info("[  TASK OUTPUT   ] Retrieved " +
                        "DeployR-encoded R " +
                        "object output " + rData.getName() +
                        " [ RStringVector ].");
                    List<String> hipNamesVal =
                        ((RStringVector) rData).getValue();
                    log.info("[  TASK OUTPUT   ] Retrieved " +
                        "DeployR-encoded R " +
                        "object " + rData.getName() +
                        " value=" + hipNamesVal);
                    /*
                     * Optionally convert RDataFrame to RTableData
                     * to simplify working with data values within
                     * the object.
                     */
                    try {
                        RDataTable table =
                            RDataFactory.createDataTable(
                                            (RStringVector) rData);
                    } catch(RDataException dex) {
                       log.warn("Unexpected str.vector to table " +
                                                "error: ex=" + dex);
                    }
                } else {
                    log.info("Unexpected DeployR-encoded R object " +
                        "returned, object name=" + rData.getName() +
                        ", encoding=" + rData.getClass());
                }
            }

        } catch(Exception ex) {
            log.warn("retrieveWorkspaceObjects: ex=" + ex);
        }
    }


    public static void retrieveFileUrls(List<URL> files,
                                       String type,
                                       String typeFilter,
                                       Logger log) {

        try {

            for(URL file : files) {

                String fileName = file.getFile().substring(file.getFile()
                                                .lastIndexOf("/")+1);

                /*
                 * If present, strip jsessionid off repository
                 * file names.
                 */
                if(fileName.indexOf(";") != -1) {
                    fileName = fileName.substring(0,
                                    fileName.indexOf(";"));
                }

                if(typeFilter == null ||
                    fileName.endsWith(typeFilter)) {
                    log.info("[  TASK OUTPUT   ] Retrieved " +  type +
                        " output " + fileName + " [ URL ]");
                    InputStream fis = null;
                    try {
                        fis = file.openStream();
                        IOUtils.toByteArray(fis);
                    } catch(Exception ex) {
                        log.warn("Working directory binary file " + ex);
                    } finally {
                        IOUtils.closeQuietly(fis);
                    }
                }
            }

        } catch(Exception ex) {
            log.warn("retrieveFileUrls: ex=" + ex);
        }
    }

    public static void retrieveProjectFiles(List<RProjectFile> files,
                                                String typeFilter,
                                                       Logger log) {

        try {

            for(RProjectFile file : files) {

                if(typeFilter == null ||
                    file.about().filename.endsWith(typeFilter)) {

                    log.info("[  TASK OUTPUT   ] Retrieved working " +
                        "directory file output " +
                        file.about().filename + " [ RProjectFile ]");
                    InputStream fis = null;
                    try {
                        fis = file.download();
                        IOUtils.toByteArray(fis);
                    } catch(Exception ex) {
                        log.warn("Working directory binary file " + ex);
                    } finally {
                        IOUtils.closeQuietly(fis);
                    }
                
                }
            }

        } catch(Exception ex) {
            log.warn("retrieveProjectFiles: ex=" + ex);
        }
    }

    public static void retrieveProjectResults(List<RProjectResult> files,
                                                           Logger log) {

        try {

            for(RProjectResult file : files) {

                log.info("[  TASK OUTPUT   ] Retrieved graphics " +
                    "device plot output " +
                    file.about().filename + " [ RProjectResult ]");
                InputStream fis = null;
                try {
                    fis = file.download();
                    IOUtils.toByteArray(fis);
                } catch(Exception ex) {
                    log.warn("Graphics device plot file " + ex);
                } finally {
                    IOUtils.closeQuietly(fis);
                }
            }

        } catch(Exception ex) {
            log.warn("retrieveProjectResults: ex=" + ex);
        }
    }

    public static void retrieveRepositoryFiles(List<RRepositoryFile> files,
                                             Logger log) {

        try {

            for(RRepositoryFile file : files) {

                log.info("[  TASK OUTPUT   ] Retrieved repository " +
                    "file output " + file.about().filename +
                    " [ RRepositoryFile ]");
                InputStream fis = null;
                try {
                    fis = file.download();
                    IOUtils.toByteArray(fis);
                } catch(Exception ex) {
                    log.warn("Repository file " + ex);
                } finally {
                    IOUtils.closeQuietly(fis);
                    if(file != null) {
                        try {
                            /*
                             * Clean up after example.
                             */
                            file.delete();
                        } catch(Exception dex) {}
                    }
                }
            }

        } catch(Exception ex) {
            log.warn("retrieveRepositoryFiles: ex=" + ex);
        }
    }

    /*
     * verifyJobExitStatus
     *
     * Test and verify that a Job has reached a given "status".
     * This implementation will busy-wait for up to 2 minutes
     * for the Job to complete, otherwise it returns failure.
     */
    public static boolean verifyJobExitStatus(RJob rJob,
                                              String status) {

        boolean verified = false;

        try {

            if(rJob != null) {

                // wait for job to complete
                int t = 120;  // Try for up to 2 minutes.
                while (t-- != 0) {

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        break;
                    }

                    try {

                         RJobDetails rJobDetails = rJob.query();

                        if (rJobDetails.status.equalsIgnoreCase(status)) {
                                /*
                                 * RJobDetails.status matches status
                                 * to be verified, break.
                                 */
                                verified = true;
                                break;
                        } else
                        if (JOB_DONE_STATES.contains(rJobDetails.status)) {
                                /*
                                 * JobDetails.status matches a "done"
                                 * state that is not the state to be
                                 * verified, so break.
                                 */
                                break;
                        }

                    } catch (Exception ex) {
                        break;
                    }
                }

                if(!verified) {
                    /*
                     * If background job has not reached a DONE state
                     * at this point, forcefully terminate it so it can
                     * be cleaned up by the example app.
                     */
                    try {
                        rJob.cancel();
                    } catch(Exception iex) { }
                }
            }

        } catch(Exception ex) {

        }

        return verified;
    }

    public static List<String> JOB_DONE_STATES = Arrays.asList(RJob.COMPLETED,
                                                               RJob.CANCELLED,
                                                               RJob.INTERRUPTED,
                                                               RJob.FAILED,
                                                               RJob.ABORTED);
    /*
     * cleanUpAfterJob
     *
     * Following execution all artifacts assocated with an
     * RJob should be removed from the DeployR database. This
     * implementation removes the RJob and the associated 
     * RProject if one was generated as a result of the RJob.
     *
     * Note, this method will not throw any Exceptions.
     */
    public static void cleanUpAfterJob(RUser rUser, String jobID) {

       try {

            RJob rJob = rUser.queryJob(jobID);

            if(rJob != null) {

                try {
                    /*
                     * Delete RJob generated RProject.
                     */
                    String projectID = rJob.about().project;
                    if(projectID != null) {
                        RProject rProject = rUser.getProject(projectID);
                        if(rProject != null) {
                            try {
                                rProject.close();
                            } catch(Exception cex) {}
                            rProject.delete();
                        }
                    }
                } catch(Exception pex) {}

               try {
                    /*
                     * Ensure RJob is stopped.
                     */
                    rJob.cancel();
                } catch(Exception cex) {}
                try {
                    /*
                     * Delete RJob.
                     */
                    rJob.delete();
                } catch(Exception dex) {}

            }

        } catch(Exception ex) {}
    }

    /*
     * cleanUpAfterRepoStore
     *
     * Following task executions that utilize the store*
     * options the example application must remove the
     * repository file that it created.
     *
     * Note, this method will not throw any Exceptions.
     */
    public static void cleanUpAfterRepoStore(RUser rUser,
                                             String filename,
                                             String directory,
                                             String author,
                                             Logger log) {

        try {

            RRepositoryFile repoFile =
                rUser.fetchFile(filename, directory, author, null);
            if(repoFile != null) {
                repoFile.delete();
            }

        } catch(Exception ex) {
            log.warn("cleanUpAfterRepoStore: failed to delete " + filename);
        }

    }

}