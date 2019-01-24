/* Copyright (C) 2016-2018 Alibaba Group Holding Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

package com.alibaba.xdl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.xdl.prerun.PreAppMaster;
import com.alibaba.xdl.SchedulerConf;
import com.alibaba.xdl.Utils;
import com.alibaba.xdl.meta.Meta.Status;

import sun.misc.Signal;
import sun.misc.SignalHandler;

@SuppressWarnings("restriction")
public class AppMasterRunner {

  private static Logger LOG = LoggerFactory.getLogger(AppMasterRunner.class);
  private static Configuration conf = new YarnConfiguration();

  public static void main(String[] args) throws Exception {
    org.apache.commons.cli.Options options = generateCLIOption();
    CommandLineParser parser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("xdl-on-yarn-applicationMaster", options);
      System.exit(1);
    }

    String config = cmd.getOptionValue("config");
    String basePath = cmd.getOptionValue("base-path");
    String user = cmd.getOptionValue("user");
    String volumes = cmd.getOptionValue("volume");

    LOG.info("AppMasterRunner cmd.parse success, config {}, basePath {}, user {}, volumes {}.", config, basePath, user, volumes);
    //AppMasterRunner cmd.parse success, config config.json, basePath hdfs://hdfscluster/user/mobdev/.xdl/application_1546428458056_0292/, user mobdev, volumes dist.tar.gz.

    SchedulerConf jobConf = Utils.parseSchedulerConf(config);
    LOG.info("AppMasterRunner jobConf, job_name {}, docker_image {}, script {}, dependent_dirs {}, scheduler_queue {}, min_finish_worker_num {}, min_finish_worker_rate {}, max_failover_times {}, max_local_failover_times {}, max_failover_wait_secs {}.", jobConf.job_name, jobConf.docker_image, jobConf.script, jobConf.dependent_dirs, jobConf.scheduler_queue, jobConf.min_finish_worker_num, jobConf.min_finish_worker_rate, jobConf.max_failover_times, jobConf.max_local_failover_times, jobConf.max_failover_wait_secs);
    // AppMasterRunner jobConf, job_name xdl_test, docker_image niumeng07/rebuild_tf_xdl_zk_delcode, script deepctr.py, dependent_dirs /home/mobdev/liuda/xdl/xdl-test-submit/dist, scheduler_queue default, min_finish_worker_num 0, min_finish_worker_rate 90.0, max_failover_times 20, max_local_failover_times 3, max_failover_wait_secs 1800.
    
    boolean balance_enable = false;
    String meta_dir = null;
    if (jobConf.auto_rebalance != null) {
      balance_enable = jobConf.auto_rebalance.enable;
      meta_dir = jobConf.auto_rebalance.meta_dir;
    }

    AppMasterBase applicationMaster;
    if (balance_enable == false || Utils.existsHdfsFile(conf, meta_dir)) { //若不开启balance_enable, 则并未判断existsHdfsFile
      LOG.info("AppMasterRunner applicationMaster is AppMasterBase."); //Here
      applicationMaster = new AppMasterBase(basePath, user, config, volumes);
    } else {
      LOG.info("AppMasterRunner applicationMaster is PreAppMaster.");
      applicationMaster = new PreAppMaster(basePath, user, config, volumes);
    }

    ApplicationMasterSignalHandler signalHandler = new ApplicationMasterSignalHandler(applicationMaster);
    Signal.handle(new Signal("TERM"), signalHandler);
    Signal.handle(new Signal("INT"), signalHandler);

    try {
      LOG.info("AppMasterRunner call applicationMaster.run().");
      Status status = applicationMaster.run();
      LOG.info("AppMasterRunner call applicationMaster.run() returns {}.", status);
      applicationMaster.dealWithExit(status);
    } catch (Exception e) {
      LOG.error("run error!", e);
      applicationMaster.dealWithExit(Status.FAIL);
    }
  }

  private static org.apache.commons.cli.Options generateCLIOption() {
    org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
    Option appStageDir = new Option("p", "base-path", true, "App stage hdfs dir to store config and scripts.");
    appStageDir.setRequired(true);
    options.addOption(appStageDir);

    Option appConfig = new Option("c", "config", true, "HDFS path of jar for xdl app config.");
    appConfig.setRequired(true);
    options.addOption(appConfig);

    Option xdlUser = new Option("u", "user", true, "user for this app.");
    xdlUser.setRequired(true);
    options.addOption(xdlUser);

    Option volumeDirInHdfs = new Option("v", "volume", true, "volume in hdfs to bind in container.");
    volumeDirInHdfs.setRequired(false);
    options.addOption(volumeDirInHdfs);

    return options;
  }

  private static class ApplicationMasterSignalHandler implements SignalHandler {
    private static Logger LOG = LoggerFactory.getLogger(ApplicationMasterSignalHandler.class);
    private AppMasterBase applicationMaster = null;

    public ApplicationMasterSignalHandler(AppMasterBase applicationMaster) {
      this.applicationMaster = applicationMaster;
    }

    @Override
    public void handle(Signal signal) {
      LOG.info("Application Master is killed by signal:{}", signal.getNumber());
      applicationMaster.dealWithExit(Status.KILLED);
    }
  }
}
