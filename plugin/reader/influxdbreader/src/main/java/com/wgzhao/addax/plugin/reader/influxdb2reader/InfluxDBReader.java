/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.reader.influxdb2reader;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.List;

public class InfluxDBReader
        extends Reader
{

    public static class Job
            extends Reader.Job
    {

        private Configuration originalConfig = null;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void preCheck()
        {
            init();
            originalConfig.getNecessaryValue(InfluxDBKey.ENDPOINT, InfluxDBReaderErrorCode.REQUIRED_VALUE);
            List<String> columns = originalConfig.getList(InfluxDBKey.COLUMN, String.class);
            String querySql = originalConfig.getString(InfluxDBKey.QUERY_SQL, null);
            String database = originalConfig.getString(InfluxDBKey.DATABASE, null);
            if (StringUtils.isAllBlank(querySql,database)) {
                throw AddaxException.asAddaxException(
                        InfluxDBReaderErrorCode.REQUIRED_VALUE,
                        "One of database or querySql must be specified"
                );
            }
            if (columns == null || columns.isEmpty()) {
                throw AddaxException.asAddaxException(
                        InfluxDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + InfluxDBKey.COLUMN + "] is not set.");
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            System.out.println("adviceNumber: " + adviceNumber);
            Configuration readerSliceConfig = super.getPluginJobConf();
            //获取输入的时间间隔
            String startTime = readerSliceConfig.getString("startTime");
            String endTime = readerSliceConfig.getString("endTime");
            //计算两个时间间隔的天数
            int days = (int) ((Long.parseLong(endTime) - Long.parseLong(startTime)) / (1000 * 3600 * 24));
            List<Configuration> splitConfigs = new ArrayList<>();
            //添加每天的任务
            for (int i = 0; i < days; i++) {
                Configuration tempConfig = readerSliceConfig.clone();
                long startTimeLong = Long.parseLong(startTime) + (long) i * 24 * 3600 * 1000;
                tempConfig.set("startTime", startTimeLong);
                tempConfig.set("endTime",startTimeLong+ 24 * 3600 * 1000);
                splitConfigs.add(tempConfig);
            }
            return splitConfigs;
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Reader.Task
    {

        private InfluxDBReaderTask influxDBReaderTask;

        @Override
        public void init()
        {
            Configuration readerSliceConfig = super.getPluginJobConf();
            this.influxDBReaderTask = new InfluxDBReaderTask(readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            this.influxDBReaderTask.startRead(recordSender, super.getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.influxDBReaderTask.post();
        }

        @Override
        public void destroy()
        {
            this.influxDBReaderTask.destroy();
        }
    }
}
