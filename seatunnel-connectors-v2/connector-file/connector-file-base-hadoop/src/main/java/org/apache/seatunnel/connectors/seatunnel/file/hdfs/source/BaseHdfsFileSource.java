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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;

import java.io.IOException;
import java.util.Objects;

public abstract class BaseHdfsFileSource extends BaseFileSource {

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        HdfsSourceConfigOptions.FILE_PATH.key(),
                        HdfsSourceConfigOptions.FILE_FORMAT_TYPE.key(),
                        HdfsSourceConfigOptions.DEFAULT_FS.key());
        if (!result.isSuccess()) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        String path = pluginConfig.getString(HdfsSourceConfigOptions.FILE_PATH.key());
        // Avoid overwriting hadoopConf for subclass initialization. If a subclass is initialized,
        // it is not initialized here.
        if (Objects.isNull(hadoopConf)) {
            hadoopConf =
                    new HadoopConf(
                            pluginConfig.getString(HdfsSourceConfigOptions.DEFAULT_FS.key()));
        }
        if (pluginConfig.hasPath(HdfsSourceConfigOptions.HDFS_SITE_PATH.key())) {
            hadoopConf.setHdfsSitePath(
                    pluginConfig.getString(HdfsSourceConfigOptions.HDFS_SITE_PATH.key()));
        }

        if (pluginConfig.hasPath(HdfsSourceConfigOptions.REMOTE_USER.key())) {
            hadoopConf.setRemoteUser(
                    pluginConfig.getString(HdfsSourceConfigOptions.REMOTE_USER.key()));
        }

        if (pluginConfig.hasPath(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL.key())) {
            hadoopConf.setKerberosPrincipal(
                    pluginConfig.getString(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL.key()));
        }
        if (pluginConfig.hasPath(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH.key())) {
            hadoopConf.setKerberosKeytabPath(
                    pluginConfig.getString(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH.key()));
        }
        readStrategy =
                ReadStrategyFactory.of(
                        pluginConfig.getString(HdfsSourceConfigOptions.FILE_FORMAT_TYPE.key()));
        readStrategy.setPluginConfig(pluginConfig);
        readStrategy.init(hadoopConf);
        try {
            filePaths = readStrategy.getFileNamesByPath(path);
        } catch (IOException e) {
            String errorMsg = String.format("Get file list from this path [%s] failed", path);
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, e);
        }

        // support user-defined schema
        FileFormat fileFormat =
                FileFormat.valueOf(
                        pluginConfig
                                .getString(HdfsSourceConfigOptions.FILE_FORMAT_TYPE.key())
                                .toUpperCase());
        // only json text csv type support user-defined schema now
        if (pluginConfig.hasPath(TableSchemaOptions.SCHEMA.key())) {
            switch (fileFormat) {
                case CSV:
                case TEXT:
                case JSON:
                case EXCEL:
                case XML:
                    SeaTunnelRowType userDefinedSchema =
                            CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
                    readStrategy.setSeaTunnelRowTypeInfo(userDefinedSchema);
                    rowType = readStrategy.getActualSeaTunnelRowTypeInfo();
                    break;
                case ORC:
                case PARQUET:
                case BINARY:
                    throw new FileConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                            "SeaTunnel does not support user-defined schema for [parquet, orc, binary] files");
                default:
                    // never got in there
                    throw new FileConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "SeaTunnel does not supported this file format");
            }
        } else {
            if (filePaths.isEmpty()) {
                // When the directory is empty, distribute default behavior schema
                rowType = CatalogTableUtil.buildSimpleTextSchema();
                return;
            }
            try {
                rowType = readStrategy.getSeaTunnelRowTypeInfo(filePaths.get(0));
            } catch (FileConnectorException e) {
                String errorMsg =
                        String.format("Get table schema from file [%s] failed", filePaths.get(0));
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED, errorMsg, e);
            }
        }
    }
}
