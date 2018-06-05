/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant;

import com.criteo.drelephant.heuristics.GarmadonTransferHeuristic;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.AnalyticJobGenerator;
import com.linkedin.drelephant.analysis.AnalyticJobGeneratorHadoop2;
import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.analysis.HadoopSystemContext;
import com.linkedin.drelephant.purge.AppResultPurger;
import com.linkedin.drelephant.security.HadoopSecurity;
import com.linkedin.drelephant.util.Utils;
import controllers.MetricsController;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import models.AppResult;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * The class that runs the Dr. Elephant daemon
 */
public class ElephantRunner implements Runnable {
  private static final Logger logger = Logger.getLogger(ElephantRunner.class);

  private static final long FETCH_INTERVAL = 60 * 1000;     // Interval between fetches
  private static final long RETRY_INTERVAL = 60 * 1000;     // Interval between retries
  private static final int EXECUTOR_NUM = 5;                // The number of executor threads to analyse the jobs

  private static final String FETCH_INTERVAL_KEY = "drelephant.analysis.fetch.interval";
  private static final String RETRY_INTERVAL_KEY = "drelephant.analysis.retry.interval";
  private static final String EXECUTOR_NUM_KEY = "drelephant.analysis.thread.count";
  private static final String RETENTION_PERIOD_DAY_KEY = "drelephant.analysis.purge.retention.period";
  private static final String PURGE_INTERVAL_SECOND_KEY = "drelephant.analysis.purge.interval";
  private static final String PURGE_BATCH_SIZE_KEY = "drelephant.analysis.purge.batch.size";
  private static final String GARMADON_TRANSFER_INTERVAL_SECOND_KEY = "drelephant.analysis.garmadon.transfer.interval";

  private AtomicBoolean _running = new AtomicBoolean(true);
  private long lastRun;
  private long _fetchInterval;
  private long _retryInterval;
  private int _executorNum;
  private HadoopSecurity _hadoopSecurity;
  private ThreadPoolExecutor _threadPoolExecutor;
  private AnalyticJobGenerator _analyticJobGenerator;

  private ScheduledExecutorService _purgeScheduler;
  private ScheduledExecutorService _garmadonTransferScheduler;

  private void loadGeneralConfiguration() {
    Configuration configuration = ElephantContext.instance().getGeneralConf();

    _executorNum = Utils.getNonNegativeInt(configuration, EXECUTOR_NUM_KEY, EXECUTOR_NUM);
    _fetchInterval = Utils.getNonNegativeLong(configuration, FETCH_INTERVAL_KEY, FETCH_INTERVAL);
    _retryInterval = Utils.getNonNegativeLong(configuration, RETRY_INTERVAL_KEY, RETRY_INTERVAL);
  }

  private void loadAnalyticJobGenerator() {
    if (HadoopSystemContext.isHadoop2Env()) {
      _analyticJobGenerator = new AnalyticJobGeneratorHadoop2();
    } else {
      throw new RuntimeException("Unsupported Hadoop major version detected. It is not 2.x.");
    }

    try {
      _analyticJobGenerator.configure(ElephantContext.instance().getGeneralConf());
    } catch (Exception e) {
      logger.error("Error occurred when configuring the analysis provider.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    logger.info("Dr.elephant has started");

    setupAppResultPurge();

    try {
      _hadoopSecurity = HadoopSecurity.getInstance();
      _hadoopSecurity.doAs(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          HDFSContext.load();
          loadGeneralConfiguration();
          loadAnalyticJobGenerator();
          ElephantContext.init();
          setupGarmadonTransferHeuristic();

          // Initialize the metrics registries.
          MetricsController.init();

          logger.info("executor num is " + _executorNum);
          if (_executorNum < 1) {
            throw new RuntimeException("Must have at least 1 worker thread.");
          }

          ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("dr-el-executor-thread-%d").build();
          _threadPoolExecutor = new ThreadPoolExecutor(_executorNum, _executorNum, 0L, TimeUnit.MILLISECONDS,
                  new LinkedBlockingQueue<Runnable>(), factory);

          while (_running.get() && !Thread.currentThread().isInterrupted()) {
            _analyticJobGenerator.updateResourceManagerAddresses();
            lastRun = System.currentTimeMillis();

            logger.info("Fetching analytic job list...");

            try {
              _hadoopSecurity.checkLogin();
            } catch (IOException e) {
              logger.info("Error with hadoop kerberos login", e);
              //Wait for a while before retry
              waitInterval(_retryInterval);
              continue;
            }

            List<AnalyticJob> todos;
            try {
              todos = _analyticJobGenerator.fetchAnalyticJobs();
            } catch (Exception e) {
              logger.error("Error fetching job list. Try again later...", e);
              //Wait for a while before retry
              waitInterval(_retryInterval);
              continue;
            }

            for (AnalyticJob analyticJob : todos) {
              _threadPoolExecutor.submit(new ExecutorJob(analyticJob));
            }

            int queueSize = _threadPoolExecutor.getQueue().size();
            MetricsController.setQueueSize(queueSize);
            logger.info("Job queue size is " + queueSize);

            //Wait for a while before next fetch
            waitInterval(_fetchInterval);
          }
          logger.info("Main thread is terminated.");
          return null;
        }
      });
    } catch (Exception e) {
      logger.error(e.getMessage());
      logger.error(ExceptionUtils.getStackTrace(e));
    }
  }

  private void setupAppResultPurge() {

    Configuration configuration = ElephantContext.instance().getGeneralConf();

    final int retentionPeriodDay = Utils.getNonNegativeInt(configuration, RETENTION_PERIOD_DAY_KEY, 0);
    final long purgeIntervalSecond = Utils.getNonNegativeLong(configuration, PURGE_INTERVAL_SECOND_KEY, 86400);//By default, do it every day
    final int purgeBatchSize = Utils.getNonNegativeInt(configuration, PURGE_BATCH_SIZE_KEY, 100);

    if (retentionPeriodDay > 0) {
      _purgeScheduler = new ScheduledThreadPoolExecutor(1);
      //Start purge with a little delay allowing application start
      _purgeScheduler.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {AppResultPurger.deleteOlderThan(retentionPeriodDay, purgeBatchSize);}

      }, 60, purgeIntervalSecond, TimeUnit.SECONDS);

      logger.info("Purge scheduled to remove results of jobs older than " + retentionPeriodDay + " days every " + purgeIntervalSecond + " seconds");
    }

  }

  private void setupGarmadonTransferHeuristic() {

    Configuration configuration = ElephantContext.instance().getGeneralConf();

    final long garmadonTransfertIntervalSecond = Utils.getNonNegativeLong(configuration,
            GARMADON_TRANSFER_INTERVAL_SECOND_KEY, 60);

    _garmadonTransferScheduler = new ScheduledThreadPoolExecutor(1);

    _garmadonTransferScheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {GarmadonTransferHeuristic.transfer();}
    }, 60, garmadonTransfertIntervalSecond, TimeUnit.SECONDS);
  }

  private class ExecutorJob implements Runnable {

    private AnalyticJob _analyticJob;

    ExecutorJob(AnalyticJob analyticJob) {
      _analyticJob = analyticJob;
    }

    @Override
    public void run() {
      try {
        String analysisName = String.format("%s %s", _analyticJob.getAppType().getName(), _analyticJob.getAppId());
        long analysisStartTimeMillis = System.currentTimeMillis();
        logger.info(String.format("Analyzing %s", analysisName));
        AppResult result = _analyticJob.getAnalysis();
        result.save();
        long processingTime = System.currentTimeMillis() - analysisStartTimeMillis;
        logger.info(String.format("Analysis of %s took %sms", analysisName, processingTime));
        MetricsController.setJobProcessingTime(processingTime);
        MetricsController.markProcessedJobs();

      } catch (InterruptedException e) {
        logger.info("Thread interrupted");
        logger.info(e.getMessage());
        logger.info(ExceptionUtils.getStackTrace(e));

        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error(e.getMessage());
        logger.error(ExceptionUtils.getStackTrace(e));

        if (_analyticJob != null && _analyticJob.retry()) {
          logger.error("Add analytic job id [" + _analyticJob.getAppId() + "] into the retry list.");
          _analyticJobGenerator.addIntoRetries(_analyticJob);
        } else {
          if (_analyticJob != null) {
            MetricsController.markSkippedJob();
            logger.error("Drop the analytic job. Reason: reached the max retries for application id = ["
                    + _analyticJob.getAppId() + "].");
          }
        }
      }
    }
  }

  private void waitInterval(long interval) {
    // Wait for long enough
    long nextRun = lastRun + interval;
    long waitTime = nextRun - System.currentTimeMillis();

    if (waitTime <= 0) {
      return;
    }

    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void kill() {
    _running.set(false);
    if (_threadPoolExecutor != null) {
      _threadPoolExecutor.shutdownNow();
    }
  }
}
