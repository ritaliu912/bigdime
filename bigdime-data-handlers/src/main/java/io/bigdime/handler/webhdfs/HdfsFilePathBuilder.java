/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.webhdfs;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidDataException;
import io.bigdime.core.constants.ActionEventHeaderConstants;

/**
 * Hdfs path will be built from hdfsPath parameter, basePath and relativePath of
 * the file. If the basePath and relativePath need to be preserved, they will be
 * concatenated with hdfsPath otherwise hdfsPath paramter will be used as the
 * final path.
 * 
 * @author Neeraj Jain
 *
 */
public class HdfsFilePathBuilder {
	private String hdfsPath;
	private String basePath;
	private String relativePath;
	private Map<String, String> tokenToHeaderNameMap;
	private ActionEvent actionEvent;
	private String partitionNames;
	private String partitionValues;
	private Map<String, String> hivePartitionNameValueMap = new LinkedHashMap<>();

	public HdfsFilePathBuilder withHdfsPath(String hdfsPath) {
		this.hdfsPath = hdfsPath;
		return this;
	}

	public HdfsFilePathBuilder withTokenHeaderMap(Map<String, String> tokenToHeaderNameMap) {
		this.tokenToHeaderNameMap = tokenToHeaderNameMap;
		return this;
	}

	private void setSourceFileBasePath() {
		basePath = "";
		if (actionEvent.getHeaders() == null)
			return;
		boolean preserveBasePath = Boolean
				.valueOf(actionEvent.getHeaders().get(ActionEventHeaderConstants.PRESERVE_BASE_PATH));
		if (!preserveBasePath)
			return;
		basePath = actionEvent.getHeaders().get(ActionEventHeaderConstants.BASE_PATH);
		if (basePath == null)
			basePath = "";
	}

	private void setSourceFileRelativePath() {
		relativePath = "";
		if (actionEvent.getHeaders() == null)
			return;
		boolean preserveBasePath = Boolean
				.valueOf(actionEvent.getHeaders().get(ActionEventHeaderConstants.PRESERVE_RELATIVE_PATH));
		if (!preserveBasePath)
			return;
		relativePath = actionEvent.getHeaders().get(ActionEventHeaderConstants.RELATIVE_PATH);
		if (relativePath == null)
			relativePath = "";
	}

	public HdfsFilePathBuilder withActionEvent(ActionEvent actionEvent) {
		this.actionEvent = actionEvent;
		return this;
	}

	public String build() throws HandlerException {
		Preconditions.checkArgument(!StringUtils.isBlank(hdfsPath), "hdfsPath can't be null or empty");
		Preconditions.checkArgument(!(actionEvent == null), "actionEvent can't be null");
		String path = buildBaseHdfsPath();
		path = redeemHdfsPathTokens(path);
		// strip the Separator, webhdfs doesn't like it if the dir ends with a
		// separator.
		if (path.endsWith(File.separator))
			path = path.substring(0, path.length() - 1);
		return path;
	}

	private String buildBaseHdfsPath() {
		Preconditions.checkArgument(!StringUtils.isBlank(hdfsPath), "hdfsPath can't be null or empty");
		setSourceFileBasePath();
		setSourceFileRelativePath();

		String path = addTrailingSlashToPath(hdfsPath);
		if (!StringUtils.isBlank(basePath)) {
			path += basePath;
			path = addTrailingSlashToPath(path);
		}

		if (!StringUtils.isBlank(relativePath)) {
			path += relativePath;
			path = addTrailingSlashToPath(path);
		}
		return path;
	}

	public String getBaseHdfsPath() {
		Preconditions.checkArgument(!StringUtils.isBlank(hdfsPath), "hdfsPath can't be null or empty");
		String baseHdfsPath = buildBaseHdfsPath();
		int $Index = hdfsPath.indexOf("${");
		if ($Index != -1) {
			baseHdfsPath = hdfsPath.substring(0, $Index);
		}
		this.hdfsPath = addTrailingSlashToPath(baseHdfsPath);
		return this.hdfsPath;
	}

	private String addTrailingSlashToPath(final String path) {
		String tempPath = path;
		if (!path.endsWith(File.separator)) {
			tempPath = path + File.separator;
		}
		if (path.contains(File.separator + File.separator))
			tempPath = path.replace(File.separator + File.separator, File.separator);
		return tempPath;
	}

	public Map<String, String> getPartitionNameValueMap() throws HandlerException {
		build();
		return Collections.unmodifiableMap(hivePartitionNameValueMap);
	}

	private String redeemHdfsPathTokens(String path) throws HandlerException {

		String detokenizedHdfsPath = addTrailingSlashToPath(path);
		if (actionEvent.getHeaders() == null) {
			return detokenizedHdfsPath;
		}
		partitionNames = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_NAMES);
		partitionValues = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_VALUES);
		if (!StringUtils.isBlank(partitionValues)) {
			String[] partitionList = partitionValues.split(",");
			// If the "partition-names" field was set, then we need to get the
			// partition names from headers
			String[] partitionNameList = null;
			if (partitionNames != null)
				partitionNameList = partitionNames.split(",");
			StringBuilder builder = new StringBuilder(detokenizedHdfsPath);
			int partitionIndex = 0;
			for (String partitionValue : partitionList) {
				builder.append(partitionValue).append(File.separator);
				if (partitionNameList != null && partitionNameList.length >= partitionIndex)
					hivePartitionNameValueMap.put(partitionNameList[partitionIndex], partitionValue);
				else
					hivePartitionNameValueMap.put(partitionValue, partitionValue);
				partitionIndex++;
			}
			detokenizedHdfsPath = builder.toString();
			return detokenizedHdfsPath;
		}
		if (tokenToHeaderNameMap == null || tokenToHeaderNameMap.isEmpty()) {
			return detokenizedHdfsPath;
		}
		int $Index = path.indexOf("${");
		if ($Index != -1) {
			for (final Entry<String, String> tokenHeaderNameEntry : tokenToHeaderNameMap.entrySet()) {
				String headerValue = actionEvent.getHeaders().get(tokenHeaderNameEntry.getValue().toUpperCase());
				if (headerValue == null) {
					throw new InvalidDataException("no header with name=" + tokenHeaderNameEntry.getValue()
							+ " found in ActionEvent. This is needed to compute the filepath on hdfs. src="
							+ actionEvent.getHeaders().get("src-desc"));
				}
				hivePartitionNameValueMap.put(tokenHeaderNameEntry.getValue(), headerValue);

				detokenizedHdfsPath = detokenizedHdfsPath.replace(tokenHeaderNameEntry.getKey(), headerValue);
			}
		}
		return detokenizedHdfsPath;
	}
}