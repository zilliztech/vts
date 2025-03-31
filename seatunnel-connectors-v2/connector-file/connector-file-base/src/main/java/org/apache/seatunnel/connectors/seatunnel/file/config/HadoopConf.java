/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;

import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;
import static org.apache.parquet.avro.AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;

@Data
public class HadoopConf implements Serializable {
    private static final String HDFS_IMPL = "org.apache.hadoop.hdfs.DistributedFileSystem";
    private static final String SCHEMA = "hdfs";
    protected Map<String, String> extraOptions = new HashMap<>();
    protected String hdfsNameKey;
    protected String hdfsSitePath;

    protected String remoteUser;

    private String krb5Path;
    protected String kerberosPrincipal;
    protected String kerberosKeytabPath;

    public HadoopConf(String hdfsNameKey) {
        this.hdfsNameKey = hdfsNameKey;
    }

    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    public String getSchema() {
        return SCHEMA;
    }

    public void setExtraOptionsForConfiguration(Configuration configuration) {
        if (!extraOptions.isEmpty()) {
            removeUnwantedOverwritingProps(extraOptions);
            extraOptions.forEach(configuration::set);
        }
        if (StringUtils.isNotBlank(hdfsSitePath)) {
            Configuration hdfsSiteConfiguration = new Configuration();
            hdfsSiteConfiguration.addResource(new Path(hdfsSitePath));
            unsetUnwantedOverwritingProps(hdfsSiteConfiguration);
            configuration.addResource(hdfsSiteConfiguration);
        }
    }

    private void removeUnwantedOverwritingProps(Map extraOptions) {
        extraOptions.remove(getFsDefaultNameKey());
        extraOptions.remove(getHdfsImplKey());
        extraOptions.remove(getHdfsImplDisableCacheKey());
    }

    public void unsetUnwantedOverwritingProps(Configuration hdfsSiteConfiguration) {
        hdfsSiteConfiguration.unset(getFsDefaultNameKey());
        hdfsSiteConfiguration.unset(getHdfsImplKey());
        hdfsSiteConfiguration.unset(getHdfsImplDisableCacheKey());
    }

    public Configuration toConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(READ_INT96_AS_FIXED, true);
        configuration.setBoolean(ADD_LIST_ELEMENT_RECORDS, false);
        configuration.setBoolean(WRITE_OLD_LIST_STRUCTURE, true);
        configuration.setBoolean(getHdfsImplDisableCacheKey(), true);
        configuration.set(getFsDefaultNameKey(), getHdfsNameKey());
        configuration.set(getHdfsImplKey(), getFsHdfsImpl());
        return configuration;
    }

    public String getFsDefaultNameKey() {
        return CommonConfigurationKeys.FS_DEFAULT_NAME_KEY;
    }

    public String getHdfsImplKey() {
        return String.format("fs.%s.impl", getSchema());
    }

    public String getHdfsImplDisableCacheKey() {
        return String.format("fs.%s.impl.disable.cache", getSchema());
    }
}
