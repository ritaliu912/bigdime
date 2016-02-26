package io.bigdime.validation;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.commons.DataConstants;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.Factory;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.Validator;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hdfs.WebHDFSConstants;
import io.bigdime.libs.hdfs.WebHdfs;
import io.bigdime.validation.common.AbstractValidator;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.DataChecksum;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Factory(id = "unzip_raw_checksum", type = UnzipRawChecksumValidator.class)
@Component

/**
 * Performs validation by comparing source zip file checksum and actual
 * checksum from hdfs.
 * 
 * @author Rita Liu
 *
 */
public class UnzipRawChecksumValidator implements Validator{
	
	private WebHdfs webHdfs;
	private static final Logger logger = LoggerFactory.getLogger(UnzipRawChecksumValidator.class);
	private String name;
	
	@Value("${archive_path}")
	protected String extractCompressedFileLocation;
	
	private boolean isReadyToValidate(final ActionEvent actionEvent) {
		String hdfsBasePath = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_PATH);
		String hdfsFileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_FILE_NAME);


		String totalSize = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_TOTAL_SIZE);
		String totalRead = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_TOTAL_READ);
		
		String readComplete = actionEvent.getHeaders().get(ActionEventHeaderConstants.READ_COMPLETE);
		if (readComplete == null || !readComplete.equalsIgnoreCase("true")) {
			logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"processing UnzipRawChecksumValidator",
					"Unzip Raw Checksum validation being skipped, totalSize={} totalRead={} hdfsBasePath={} hdfsFileName={} readComplete={}",
					totalSize, totalRead, hdfsBasePath, hdfsFileName, readComplete);
			return false;
		}
		return true;
	}
	@Override
	public ValidationResponse validate(ActionEvent actionEvent)
			throws DataValidationException {
		ValidationResponse validationPassed = new ValidationResponse();
		AbstractValidator commonValidator = new AbstractValidator();
		validationPassed.setValidationResult(ValidationResult.FAILED);
		String host = actionEvent.getHeaders().get(ActionEventHeaderConstants.HOST_NAMES);
		String portString = actionEvent.getHeaders().get(ActionEventHeaderConstants.PORT);
		String hdfsPath = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_PATH);
		String hdfsFileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_FILE_NAME);
		String sourceFilePath = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_PATH);
		String sourceFileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_NAME);
		String hivePartitionValues = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_VALUES);
		String partitionPath = "";
		String hdfsCompletedPath = "";
		String sourceFileChecksum = "";
		
		if (!isReadyToValidate(actionEvent)) {
			validationPassed.setValidationResult(ValidationResult.NOT_READY);
			return validationPassed;
		}
		
		commonValidator.checkNullStrings(ActionEventHeaderConstants.HOST_NAMES, host);
		commonValidator.checkNullStrings(ActionEventHeaderConstants.PORT, portString);
		
		try {
			int port = Integer.parseInt(portString);
			if (webHdfs == null) {
				webHdfs = WebHdfs.getInstance(host, port);
			}
			webHdfs.addParameter(WebHDFSConstants.USER_NAME, "hdfs");
		} catch (NumberFormatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "NumberFormatException",
					"Illegal port number input while parsing string to integer");
			throw new NumberFormatException();
		}
		
		commonValidator.checkNullStrings(ActionEventHeaderConstants.HDFS_PATH, hdfsPath);
		if(!StringUtils.endsWith(hdfsPath, DataConstants.SLASH)){
			StringBuilder pathBuilder = new StringBuilder();
			pathBuilder.append(hdfsPath).append(DataConstants.SLASH);
			hdfsPath = pathBuilder.toString();
		}
		commonValidator.checkNullStrings(ActionEventHeaderConstants.HDFS_FILE_NAME, hdfsFileName);
		commonValidator.checkNullStrings(ActionEventHeaderConstants.SOURCE_FILE_PATH, sourceFilePath);
		commonValidator.checkNullStrings(ActionEventHeaderConstants.SOURCE_FILE_NAME, sourceFileName);
		
		try {
			if (!new File(sourceFilePath).exists()) {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "File Not Found",
						"Source zip file does not exist : " + sourceFilePath, FileNotFoundException.class);
			} else {
					sourceFileChecksum = getSourceZipFileChecksum(sourceFilePath, sourceFileName);
			}
		} catch (IOException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "IOException",
					"Exception occurred while getting source file raw checksum", e);
			throw new DataValidationException("IOException while getting source raw checksum for " + sourceFilePath);
		} catch (URISyntaxException ex) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "URISyntaxException",
					"Exception occurred while getting source file raw checksum", ex);
			throw new DataValidationException("URISyntaxException while getting source raw checksum for " + sourceFilePath);
		}
		if (sourceFileChecksum.length() > 0) {

			if (StringUtils.isNotBlank(hivePartitionValues)) {
				String[] partitionList = hivePartitionValues.split(DataConstants.COMMA);
				StringBuilder stringBuilder = new StringBuilder();
				for (int i = 0; i < partitionList.length; i++) {
					stringBuilder.append(partitionList[i].trim() + DataConstants.SLASH);
				}
				partitionPath = stringBuilder.toString();
				hdfsCompletedPath = hdfsPath + partitionPath + hdfsFileName;
			} else {
				hdfsCompletedPath = hdfsPath + hdfsFileName;
			}
			String hdfsFileChecksum = "";
			try {
				hdfsFileChecksum = getHdfsFileChecksum(hdfsCompletedPath);
			} catch (ClientProtocolException e) {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "ClientProtocolException",
						"Exception occurred while getting hdfs raw checksum for {}, cause: {}", hdfsCompletedPath, e.getCause());
				throw new DataValidationException("ClientProtocolException while getting hdfs raw checksum for hdfs file " + hdfsCompletedPath);
			} catch (IOException ex) {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "IOException",
						"Exception occurred while getting hdfs raw checksum for {}, cause: {}", hdfsCompletedPath, ex.getMessage());
				throw new DataValidationException("IOException while getting hdfs raw checksum for " + hdfsCompletedPath);
			}

			if (hdfsFileChecksum.length() > 0) {

				if (sourceFileChecksum.equals(hdfsFileChecksum)) {
					logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
							"Unzip Raw Checksum matches", "Hdfs file raw checksum ({}) is same as source file raw checksum ({}). hdfsFileName = {}", 
							sourceFileChecksum, hdfsFileChecksum, hdfsCompletedPath);
					validationPassed.setValidationResult(ValidationResult.PASSED);
				} else {
					
					String checksumErrorFilePath = "ChecksumError" + DataConstants.SLASH
							+ AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName() + DataConstants.SLASH + partitionPath;
					// take out /webhdfs/v1 from hdfsBasePath for building
					// checksumErrorFilePath
					String hdfsDir = hdfsPath.substring(11);
					try {
						if (!checkErrorChecksumDirExists(hdfsPath + checksumErrorFilePath)) {
							if (makeErrorChecksumDir(hdfsPath + checksumErrorFilePath)) {

								moveErrorChecksumFile(hdfsCompletedPath, hdfsDir + checksumErrorFilePath);
							}
						} else {

							moveErrorChecksumFile(hdfsCompletedPath, hdfsDir + checksumErrorFilePath);
						}

					} catch (IOException e) {
						logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
								"Exception occurs",
								"Failed to move the file {} to provided location: {} ", hdfsCompletedPath, hdfsDir + checksumErrorFilePath);
						throw new DataValidationException("IOException while moving the file " + hdfsCompletedPath + " to " + hdfsDir+checksumErrorFilePath);
					}
					
					logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
							"Unzip Raw Checksum mismatches HDFS file moved",
							"Hdfs file raw checksum is different as source file raw checksum, hdfs file {} moved to {}. sourceFileChecksum={} hdfsFileChecksum={}", 
							hdfsCompletedPath, hdfsPath + checksumErrorFilePath, sourceFileChecksum, hdfsFileChecksum);
					validationPassed.setValidationResult(ValidationResult.FAILED);
				}
			} else {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "WARNING",
						"Hdfs file checksum {} cannot be calculated.", hdfsCompletedPath);
				validationPassed.setValidationResult(ValidationResult.FAILED);
			}
		} else {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "WARNING",
					"Source file checksum {} cannot be calculated.", sourceFilePath);
			validationPassed.setValidationResult(ValidationResult.FAILED);
		}
		return validationPassed;
	}
	
	/**
	 * This method is getting hdfs file raw checksum using WebHdfs
	 * 
	 * @param hdfsFilePath
	 * @return hdfs raw checksum string
	 * @throws ClientProtocolException
	 * @throws IOException
	 */

	private String getHdfsFileChecksum(String hdfsFilePath) throws ClientProtocolException, IOException {
		HttpResponse response = webHdfs.checksum(hdfsFilePath);
		InputStream responseStream = response.getEntity().getContent();
		StringBuilder stringBuilder = new StringBuilder();
		BufferedReader buffer = new BufferedReader(new InputStreamReader(responseStream, "UTF-8"));
		String line = null;
		while ((line = buffer.readLine()) != null) {
			stringBuilder.append(line);
		}
		responseStream.close();
		buffer.close();
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode jsonNode = objectMapper.readTree(stringBuilder.toString());
		JsonNode fileChecksum = jsonNode.get("FileChecksum");
		webHdfs.releaseConnection();
		String hdfsFileChecksum = fileChecksum.get("bytes").toString();
		hdfsFileChecksum = hdfsFileChecksum.substring(25, 57).replace("\"", "");
		return hdfsFileChecksum;
	}

	/**
	 * This method is to unzip and calculate source file raw checksum
	 * 
	 * @param sourceFilePath
	 * @param fileName
	 * @return source file raw checksum string
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws DataValidationException 
	 */
	public String getSourceZipFileChecksum(String sourceFilePath, String fileName) throws IOException, URISyntaxException {
		
		File exctractFileDir = new File(extractCompressedFileLocation);
		boolean isDirCreated = false;
		String sourceCompletePath ="";

		if (!exctractFileDir.exists()) {
			isDirCreated = new File(extractCompressedFileLocation).mkdir();
			if(!isDirCreated){
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"Unable to create extract temporary directory", "extract tmp dir = {}", extractCompressedFileLocation);
			}
		} else {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), 
					"Extract compressed file location already existed", "extract tmp dir = {}", extractCompressedFileLocation);
		}

		String localFileDir = extractCompressedFileLocation
				+ sourceFilePath.substring(sourceFilePath.lastIndexOf(DataConstants.SLASH)+1, sourceFilePath.lastIndexOf("."))
				+ DataConstants.SLASH;
		
		File localFileDirFile = new File(localFileDir);
		if (!localFileDirFile.exists()) {
			isDirCreated = new File(localFileDir).mkdir();
			if(!isDirCreated){
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"Unable to create extract temporary file: {}", localFileDir);
			}
		}	
		
		unZipFile(new File(sourceFilePath), localFileDir, fileName, DataValidationConstants.BUFFER_SIZE_1024);
		sourceCompletePath = localFileDir + fileName;
		
		Configuration conf = new Configuration();
		MD5MD5CRC32FileChecksum md5Checksum = null;
		int bytesPerCRC = DataValidationConstants.CHECKSUM_BYTE_PER_CRC;
		int blockSize = DataValidationConstants.CHECKSUM_BLOCK_SIZE;
		Path srcPath = new Path(sourceCompletePath);
		FileSystem srcFs = LocalFileSystem.get(new URI(sourceCompletePath), conf);
		long fileSize = 0;
		int blockCount = 0;
		DataOutputBuffer md5outDataBuffer = new DataOutputBuffer();
		DataChecksum checksum = DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C,
				DataValidationConstants.CHECKSUM_BYTE_PER_CRC);
		InputStream inputStream = null;
		long crc_per_block = blockSize / bytesPerCRC;
		fileSize = srcFs.getFileStatus(srcPath).getLen();
		blockCount = (int) Math.ceil((double) fileSize / (double) blockSize);

		inputStream = srcFs.open(srcPath);
		long totalBytesRead = 0;

		for (int x = 0; x < blockCount; x++) {
			ByteArrayOutputStream ar_CRC_Bytes = new ByteArrayOutputStream();
			byte crc[] = new byte[DataValidationConstants.BUFFER_SIZE_4];
			byte buf[] = new byte[DataValidationConstants.BUFFER_SIZE_512];

			int bytesRead = 0;
			while ((bytesRead = inputStream.read(buf)) > 0) {
				totalBytesRead += bytesRead;
				checksum.reset();
				checksum.update(buf, 0, bytesRead);
				checksum.writeValue(crc, 0, true);
				ar_CRC_Bytes.write(crc);
				if (totalBytesRead >= (long) (x + 1) * blockSize) {
					break;
				}
			}
			DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(ar_CRC_Bytes.toByteArray()));

			final MD5Hash md5_dataxceiver = MD5Hash.digest(dataInputStream);
			md5_dataxceiver.write(md5outDataBuffer);

			ar_CRC_Bytes.close();
			dataInputStream.close();

		}
		MD5Hash md5_of_md5 = MD5Hash.digest(md5outDataBuffer.getData());
		md5Checksum = new MD5MD5CRC32FileChecksum(bytesPerCRC, crc_per_block, md5_of_md5);

		inputStream.close();
		srcFs.close();
		String localFileChecksum = md5Checksum.toString().split(DataConstants.COLON)[1];
		deleteTemporaryDir(localFileDir);
		return localFileChecksum;
	}
	
	/**
	 * This method is to unzip the file and write to unZipFilePath
	 * 
	 * @param zipFilePath
	 * @param unZipFilePath
	 * @param fileName
	 * @param readBufferSize
	 * @throws IOException
	 * @throws DataValidationException
	 */
	private void unZipFile(File zipFilePath, String unZipFilePath,
			String fileName, int readBufferSize) throws IOException{
		byte[] buffer = new byte[readBufferSize];
		ZipInputStream zis = null;
		FileOutputStream out = null;
		ZipEntry ze;
		String srcFileName = "";
		int len;
		try {
			//check if source location exists or not, if not, stop and return
			if(!zipFilePath.exists()){
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
						"Zip File Not Found", "Zip File {} is not found, cannot unzip", zipFilePath.toString());
				return;
			}
			zis = new ZipInputStream(new FileInputStream(zipFilePath));
			
			//check if there has contents in zip file or not, if not, stop and return
			if((ze = zis.getNextEntry()) == null){
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
						"No Content in Zip", "There is no content in zip file {}", zipFilePath.toString());
				zis.close();
				return;
			}
			srcFileName = ze.getName();
			if(ze.getName().contains(DataConstants.SLASH)){
				srcFileName = ze.getName().substring(ze.getName().lastIndexOf(DataConstants.SLASH)+1);	
			}
			//check if file exists in zip or not, if not, stop and return
			if(!fileName.equals(srcFileName)){
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
						"No File Found in Zip", "File {} is not found in zip file {}", fileName, zipFilePath.toString());
				zis.close();
				return;
			}
			//check if file has contents or not, if not, stop and return
			if(ze.getSize()==0){
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
						"No Content Found in File", "File {} has no content in zip file {}", fileName, zipFilePath.toString());
				zis.close();
				return;
			}
			while (ze != null) {
				File newFile = new File(unZipFilePath + fileName);
				if (fileName.equals(srcFileName)) {
					out = new FileOutputStream(newFile);
					while ((len = zis.read(buffer)) > 0) {
						out.write(buffer, 0, len);
					}			
				}
				ze=zis.getNextEntry();
			}
			zis.close();
			out.close();
		} catch (IOException ex) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"IOException", "Exception occurred during unzip file {}" + zipFilePath.toString(), ex.getCause());
		} finally {
			if (zis != null) {
				try {
					zis.close();
				} catch (IOException e) {
					logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
							"IOException", "Exception occurred during unzip file {}" + zipFilePath.toString(), e.getCause());
				}
			}
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
							"IOException", "Exception occurred during unzip file {}" + zipFilePath.toString(), e.getCause());
				}
			}
		}
	}
	
	/**
	 * This is to check errorChecksumDir exists or not in hdfs
	 * 
	 * @param filePath
	 * @return true if errorChecksumDir exists else return false
	 * @throws IOException
	 */
	private boolean checkErrorChecksumDirExists(String filePath) throws IOException {
		HttpResponse response = webHdfs.fileStatus(filePath);
		webHdfs.releaseConnection();
		if (response.getStatusLine().getStatusCode() == 404) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * This is to make error checksum directory in hdfs
	 * 
	 * @param dirPath
	 * @return true if create directory successfully else return false
	 * @throws IOException
	 */
	private boolean makeErrorChecksumDir(String dirPath) throws IOException {
		HttpResponse response = webHdfs.mkdir(dirPath);
		webHdfs.releaseConnection();
		if (response.getStatusLine().getStatusCode() == 200) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This method is move errorChecksumFile to another location in hdfs
	 * 
	 * @param source
	 *            errorChecksumFile path
	 * @param dest
	 *            error checksum directory
	 * @throws IOException
	 */
	private void moveErrorChecksumFile(String source, String dest) throws IOException {
		webHdfs.addParameter("destination", dest);
		webHdfs.rename(source);
		webHdfs.releaseConnection();
	}
	
	private void deleteTemporaryDir(String tmpDir){
		String localFileDirWithoutSlash ="";
		// delete temporary directory
		try {
			// delete temporary directory
			localFileDirWithoutSlash = tmpDir.substring(0,
					tmpDir.lastIndexOf("/"));
			FileDeleteStrategy.FORCE.delete(new File(localFileDirWithoutSlash));
		} catch (Throwable e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "WARNING", "Unable to delete temporary directory {}" + localFileDirWithoutSlash, e);
		}
	}
	
	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
