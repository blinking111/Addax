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

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;

public class InfluxDBReaderTask {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBReaderTask.class);

    private static final int CONNECT_TIMEOUT_SECONDS_DEFAULT = 15;
    private static final int SOCKET_TIMEOUT_SECONDS_DEFAULT = 20;

    private final String querySql;
    private final String database;
    private final String endpoint;
    private final String username;
    private final String password;

    private final int connTimeout;
    private final int socketTimeout;


    private final Long startTime;

    private final Long endTime;

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public InfluxDBReaderTask(Configuration configuration) {
        List<Object> connList = configuration.getList(InfluxDBKey.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        this.querySql = configuration.getString(InfluxDBKey.QUERY_SQL, null);
        this.database = conn.getString(InfluxDBKey.DATABASE);
        this.endpoint = conn.getString(InfluxDBKey.ENDPOINT);
        this.username = configuration.getString(InfluxDBKey.USERNAME);
        this.password = configuration.getString(InfluxDBKey.PASSWORD, null);
        this.connTimeout = configuration.getInt(InfluxDBKey.CONNECT_TIMEOUT_SECONDS, CONNECT_TIMEOUT_SECONDS_DEFAULT) * 1000;
        this.socketTimeout = configuration.getInt(InfluxDBKey.SOCKET_TIMEOUT_SECONDS, SOCKET_TIMEOUT_SECONDS_DEFAULT) * 1000;
        this.startTime = configuration.getLong("startTime");
        this.endTime = configuration.getLong("endTime");
    }

    public void post() {
        //
    }

    public void destroy() {
        //
    }

    public void startRead(RecordSender recordSender, TaskPluginCollector taskPluginCollector) {
        LOG.info("connect influxdb: {} with username: {}", endpoint, username);

        String tail = "/query";
        String enc = "utf-8";
        String result = null;
        int offset = 0;
        try {
            boolean hasMore = false;
            int count = 0;
            int seriesCount = 0;
            do {
                hasMore = false;
                String url = endpoint + tail + "?db=" + URLEncoder.encode(database, enc);
                if (!"".equals(username)) {
                    url += "&u=" + URLEncoder.encode(username, enc);
                }
                if (!"".equals(password)) {
                    url += "&p=" + URLEncoder.encode(password, enc);
                }
                url += "&epoch=ms";
//            if (querySql.contains("#lastMinute#")) {
//                this.querySql = querySql.replace("#lastMinute#", getLastMinute());
//            }
                String querySql= this.querySql;
                if (querySql.contains("#startTime#")) {
                    querySql = querySql.replace("#startTime#", sdf.format(new Date(startTime)));
                }
                if (querySql.contains("#endTime#")) {
                    querySql = querySql.replace("#endTime#", sdf.format(new Date(endTime)));
                }
                if (querySql.contains("#offset#")) {
                    querySql = querySql.replace("#offset#", String.valueOf(offset));
                }

                url += "&q=" + URLEncoder.encode(querySql, enc);
                result = get(url);
                if (StringUtils.isBlank(result)) {
//            throw AddaxException.asAddaxException(
//                    InfluxDBReaderErrorCode.ILLEGAL_VALUE, "Get nothing!", null);
                    LOG.error("Get nothing!");
                }
                try {
                    JSONObject jsonObject = JSONObject.parseObject(result);
                    JSONArray results = (JSONArray) jsonObject.get("results");
                    JSONObject resultsMap = (JSONObject) results.get(0);
                    if (resultsMap.containsKey("series")) {
                        JSONArray series = (JSONArray) resultsMap.get("series");
                        for (Object o : series) {
                            seriesCount++;
                            JSONObject seriesMap = (JSONObject) o;
                            if (seriesMap.containsKey("values")) {
                                JSONArray values = (JSONArray) seriesMap.get("values");
                                for (Object row : values) {
                                    JSONArray rowArray = (JSONArray) row;
                                    Record record = recordSender.createRecord();
                                    int j = 0;
                                    for (Object s : rowArray) {
                                        if (null != s) {
                                            //时间戳转换
                                            if (j == 0) {
                                                record.addColumn(new StringColumn(sdf.format(new Date(Long.parseLong(s.toString())))));
                                            } else {
                                                record.addColumn(new StringColumn(s.toString()));
                                            }
                                        } else {
                                            record.addColumn(new StringColumn());
                                        }
                                        j++;
                                    }
                                    hasMore = true;
                                    count++;
                                    recordSender.sendToWriter(record);
                                }
                            }
                        }
                    } else if (resultsMap.containsKey("error")) {
//                throw AddaxException.asAddaxException(
//                        InfluxDBReaderErrorCode.ILLEGAL_VALUE, "Error occurred in data sets！", null);
                        LOG.error("Error occurred in data sets！result:{}", result);
                    }
                } catch (Exception e) {
//            throw AddaxException.asAddaxException(
//                    InfluxDBReaderErrorCode.ILLEGAL_VALUE, "Failed to send data", e);
                    LOG.error("Failed to send data", e);
                }
                offset++;
                LOG.info("当前时间：" + sdf.format(new Date(startTime)) + "当前位移：" + offset + "是否有下一次请求：" + hasMore + "当前条数：" + count + "当前处理seiies数：" + seriesCount);
            } while (hasMore);


        } catch (Exception e) {
//            throw AddaxException.asAddaxException(
//                    InfluxDBReaderErrorCode.ILLEGAL_VALUE, "Failed to get data point！", e);
            LOG.error("Failed to get data point！", e);
        }


    }

    public String get(String url)
            throws Exception {
        Content content = Request.Get(url)
                .connectTimeout(this.connTimeout)
                .socketTimeout(this.socketTimeout)
                .execute()
                .returnContent();
        if (content == null) {
            return null;
        }
        return content.asString(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("JavaTimeDefaultTimeZone")
    private String getLastMinute() {
        long lastMinuteMilli = LocalDateTime.now().plusMinutes(-1).toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return String.valueOf(lastMinuteMilli);
    }
}
