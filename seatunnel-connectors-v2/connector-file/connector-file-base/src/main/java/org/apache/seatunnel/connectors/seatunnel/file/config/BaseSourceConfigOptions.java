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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import java.util.List;
import java.util.Map;

public class BaseSourceConfigOptions {
    public static final Option<FileFormat> FILE_FORMAT_TYPE =
            Options.key("file_format_type")
                    .objectType(FileFormat.class)
                    .noDefaultValue()
                    .withDescription(
                            "File format type, e.g. json, csv, text, parquet, orc, avro....");

    public static final Option<String> FILE_PATH =
            Options.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The file path of source files");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(TextFormatConstant.SEPARATOR[0])
                    .withFallbackKeys("delimiter")
                    .withDescription(
                            "The separator between columns in a row of data. Only needed by `text` file format");

    public static final Option<String> ENCODING =
            Options.key("encoding")
                    .stringType()
                    .defaultValue("UTF-8")
                    .withDescription(
                            "The encoding of the file to read, e.g. UTF-8, ISO-8859-1....");

    public static final Option<DateUtils.Formatter> DATE_FORMAT =
            Options.key("date_format")
                    .enumType(DateUtils.Formatter.class)
                    .defaultValue(DateUtils.Formatter.YYYY_MM_DD)
                    .withDescription("Date format");

    public static final Option<DateTimeUtils.Formatter> DATETIME_FORMAT =
            Options.key("datetime_format")
                    .enumType(DateTimeUtils.Formatter.class)
                    .defaultValue(DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS)
                    .withDescription("Datetime format");

    public static final Option<TimeUtils.Formatter> TIME_FORMAT =
            Options.key("time_format")
                    .enumType(TimeUtils.Formatter.class)
                    .defaultValue(TimeUtils.Formatter.HH_MM_SS)
                    .withDescription("Time format");

    public static final Option<Boolean> PARSE_PARTITION_FROM_PATH =
            Options.key("parse_partition_from_path")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether parse partition fields from file path");

    public static final Option<String> HDFS_SITE_PATH =
            Options.key("hdfs_site_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The path of hdfs-site.xml");

    public static final Option<String> REMOTE_USER =
            Options.key("remote_user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The remote user name of hdfs");

    public static final Option<String> KERBEROS_PRINCIPAL =
            Options.key("kerberos_principal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kerberos principal");

    public static final Option<String> KRB5_PATH =
            Options.key("krb5_path")
                    .stringType()
                    .defaultValue("/etc/krb5.conf")
                    .withDescription(
                            "When use kerberos, we should set krb5 path file path such as '/seatunnel/krb5.conf' or use the default path '/etc/krb5.conf");

    public static final Option<String> KERBEROS_KEYTAB_PATH =
            Options.key("kerberos_keytab_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kerberos keytab file path");

    public static final Option<Long> SKIP_HEADER_ROW_NUMBER =
            Options.key("skip_header_row_number")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("The number of rows to skip");

    public static final Option<List<String>> READ_PARTITIONS =
            Options.key("read_partitions")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The partitions that the user want to read");

    public static final Option<List<String>> READ_COLUMNS =
            Options.key("read_columns")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The columns list that the user want to read");

    public static final Option<String> SHEET_NAME =
            Options.key("sheet_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("To be read sheet name,only valid for excel files");

    public static final Option<String> XML_ROW_TAG =
            Options.key("xml_row_tag")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specifies the tag name of the data rows within the XML file, only valid for XML files.");

    public static final Option<Boolean> XML_USE_ATTR_FORMAT =
            Options.key("xml_use_attr_format")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Specifies whether to process data using the tag attribute format, only valid for XML files.");

    public static final Option<String> FILE_FILTER_PATTERN =
            Options.key("file_filter_pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "File pattern. The connector will filter some files base on the pattern.");

    public static final Option<CompressFormat> COMPRESS_CODEC =
            Options.key("compress_codec")
                    .enumType(CompressFormat.class)
                    .defaultValue(CompressFormat.NONE)
                    .withDescription("Compression codec");

    public static final Option<ArchiveCompressFormat> ARCHIVE_COMPRESS_CODEC =
            Options.key("archive_compress_codec")
                    .enumType(ArchiveCompressFormat.class)
                    .defaultValue(ArchiveCompressFormat.NONE)
                    .withDescription("Archive compression codec");

    public static final Option<List<Map<String, Object>>> TABLE_CONFIGS =
            Options.key("tables_configs")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "Local file source configs, used to create multiple local file source.");
}
