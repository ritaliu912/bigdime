package io.bigdime.handler.file;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.DataConstants;
import io.bigdime.core.commons.FileHelper;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.apache.commons.lang.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


/**
 * A handler that reads data from a file. The data is read in binary or text
 * depending on the handler configuration.
 * @formatter:off
 * Read a file from given location.
 * Read the location of the file.
 * Read the buffer size.
 * if context is null
 * 	start_index=0;
 * else
 * 	start_index=bytes_read_already
 *
 *
 * File:
 *
 * Use multiple channels if:
 * reader is faster
 * record boundary
 * order of records doesn't matter.
 *
 * Use single outputChannel if any of the below is true:
 * checksum is required
 * order or the records need to be maintained
 * @formatter:on
 * @author Neeraj Jain, Rita Liu
 *
 */
@Component
@Scope("prototype")
public class ZipFileInputStreamHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(ZipFileInputStreamHandler.class));
	public static final String FILE_LOCATION = "fileLocation";

	private static int DEFAULT_BUFFER_SIZE = 1024 * 1024;

	/**
	 * Applicable in case of a text file. This is ignored if the datatype is
	 * binary.
	 */
	// private String encoding;

	/**
	 * Does the file has lines? If yes, what's the delimiting character. If no,
	 * it should be null.
	 */
	// private String lineDelimiter;

	/**
	 * Does the order of records need to be preserved?
	 */
	// private boolean preserveOrder;

//	private RandomAccessFile file;

	private File currentFile;

	private String fileNamePattern;

	private int bufferSize;

	/**
	 * Base path, absolute, to look for the files.
	 */
	private String fileLocation;
	// private String srcDescFilePath;

	private long fileLength = -1;

//	private FileChannel fileChannel;

	private FileInputDescriptor fileInputDescriptor;

	// private List<String> availableFilesInFolder = new ArrayList<>();

	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;
	@Autowired
	private FileHelper fileHelper;

	private String handlerPhase = "";
	private String entityName;

	private File currentFileToProcess;
	private int readEntries = 0;
	private String basePath = "/";
	private boolean preserveBasePath;
	private boolean preserveRelativePath;
	private ZipEntry zipFileEntry;
	private ZipFile zipFile;
	private InputStream inputStream;
	private BufferedInputStream bufferedInputStream;
	private int noOfEntry;
	private String fileName = "";
	private long totalRead = 0;
	private long fileSize = 0;
	Enumeration<? extends ZipEntry> entriesInZip;
		
	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		handlerPhase = "building ZipFileInputStreamHandler";
		logger.info(handlerPhase, "properties={}", getPropertyMap());

		@SuppressWarnings("unchecked")
		Entry<String, String> srcDescInputs = (Entry<String, String>) getPropertyMap()
				.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
		if (srcDescInputs == null) {
			throw new InvalidValueConfigurationException("src-desc can't be null");
		}
		logger.debug(handlerPhase, "entity:fileNamePattern={} input_field_name={}", srcDescInputs.getKey(),
				srcDescInputs.getValue());

		basePath = PropertyHelper.getStringProperty(getPropertyMap(), FileInputStreamReaderHandlerConstants.BASE_PATH,
				DataConstants.SLASH);
		logger.debug(handlerPhase, "basePath={}", basePath);

		fileInputDescriptor = new FileInputDescriptor();
		try {
			fileInputDescriptor.parseDescriptor(basePath);
		} catch (IllegalArgumentException ex) {
			throw new InvalidValueConfigurationException(
					"src-desc must contain entityName:fileNamePrefix, e.g. user:web_user*");
		}

		fileLocation = fileInputDescriptor.getPath();
		if (fileLocation == null) {
			throw new InvalidValueConfigurationException("filePath in src-desc can't be null");
		}

		String[] srcDesc = fileInputDescriptor.parseSourceDescriptor(srcDescInputs.getKey());

		entityName = srcDesc[0];
		fileNamePattern = srcDesc[1];
		logger.debug(handlerPhase, "entityName={} fileNamePattern={}", entityName, fileNamePattern);

		bufferSize = PropertyHelper.getIntProperty(getPropertyMap(), FileInputStreamReaderHandlerConstants.BUFFER_SIZE,
				DEFAULT_BUFFER_SIZE);
		logger.debug(handlerPhase, "id={} fileLocation={}", getId(), fileLocation);
		preserveBasePath = PropertyHelper.getBooleanProperty(getPropertyMap(),
				FileInputStreamReaderHandlerConstants.PRESERVE_BASE_PATH);
		preserveRelativePath = PropertyHelper.getBooleanProperty(getPropertyMap(),
				FileInputStreamReaderHandlerConstants.PRESERVE_RELATIVE_PATH);

		logger.debug(handlerPhase, "id={}", getId());
	}

	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing ZipFileInputStreamHandler";
		try {
			Status status = preProcess();
			if (status == Status.BACKOFF) {
				logger.debug(handlerPhase, "returning BACKOFF");
				return status;
			}
			return doProcess();
		} catch (IOException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"error during reading file", e);
			throw new HandlerException("Unable to process message from file", e);
		} catch (RuntimeInfoStoreException e) {
			throw new HandlerException("Unable to process message from file", e);
		}
	}

	private Status preProcess() throws IOException, RuntimeInfoStoreException, HandlerException {

		if (readAllFromZip() && readAllFromFile()) {
			currentFileToProcess = getNextFileToProcess();
			if (currentFileToProcess == null) {			
				logger.info(handlerPhase, "_message=\"no file to process\" handler_id={} ", getId());
				return Status.BACKOFF;
			}
			zipFile = new ZipFile(currentFileToProcess);
			noOfEntry = getNumberOfFileInZip(currentFileToProcess);
			readEntries++;
			fileLength = currentFileToProcess.length();
			logger.info(handlerPhase, "_message=\"got a new file to process\" handler_id={} file_length={}", getId(),
					fileLength);
			if (fileLength == 0) {
				logger.info(handlerPhase, "_message=\"file is empty\" handler_id={} ", getId());
				return Status.BACKOFF;
			}
			logger.info(handlerPhase, "_message=\"number files in zip\" handler_id={} no_of_file={} ", getId(),noOfEntry);
			getZipFileHandlerJournal().setTotalEntries(noOfEntry);
			getZipFileHandlerJournal().setZipFileName(currentFileToProcess.getName());
			getZipFileHandlerJournal().setReadEntries(readEntries);
			return Status.READY;
		}
		return Status.READY;
	}

	private Status doProcess() throws IOException, HandlerException, RuntimeInfoStoreException {
		
		byte[] data ;
		long bytesReadCount=0;
		int readSize =0;
		String srcFileName = "";
		
		logger.debug(handlerPhase, "handler_id={} next_index_to_read={} buffer_size={}", getId(), getTotalReadFromJournal(),
				bufferSize);
		if(getReadEntriesFromJournal() == 1 && readAllFromFile()){
			entriesInZip = zipFile.entries();
		}
		if(readAllFromFile()){
			fileName = fileNameFromZip(entriesInZip);
			if(fileName.contains(DataConstants.SLASH)){
				srcFileName = fileName.substring(fileName.lastIndexOf(DataConstants.SLASH)+1);	
			}else{
				srcFileName = fileName;
			}
			getZipFileHandlerJournal().setEntryName(fileName);
			getZipFileHandlerJournal().setSrcFileName(srcFileName);
			getZipFileHandlerJournal().setTotalSize(zipFileEntry.getSize());
			inputStream = zipFile.getInputStream(zipFileEntry);
			bufferedInputStream = new BufferedInputStream(inputStream);
			fileSize = zipFileEntry.getSize();
		}
		
		logger.info(
				handlerPhase,
				"_message=\"File details\" handler_id={}  file_name={} file_size={} ",
				getId(), fileName, zipFileEntry.getSize());
		
		if(fileSize < bufferSize){
			readSize = (int) fileSize;
		}else{
			readSize = bufferSize;
		}
		
		data = new byte[readSize];
		bytesReadCount = bufferedInputStream.read(data, 0, readSize);
		if(bytesReadCount > 0){
			totalRead = totalRead + bytesReadCount;
			getZipFileHandlerJournal().setTotalRead(totalRead);
		
			ActionEvent outputEvent = new ActionEvent();
			logger.debug(handlerPhase,
				"handler_id={} readBody.length={} remaining={}", getId(),
				bytesReadCount, (fileSize - bytesReadCount));

			outputEvent.setBody(data);
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.INPUT_DESCRIPTOR,
				currentFile.getAbsolutePath());
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.SOURCE_FILE_PATH,
				currentFile.getAbsolutePath());
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.SOURCE_FILE_NAME,
				getZipFileHandlerJournal().getSrcFileName());
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.SOURCE_FILE_TOTAL_SIZE,
				String.valueOf(getTotalSizeFromJournal()));
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.SOURCE_FILE_TOTAL_READ,
				String.valueOf(getTotalReadFromJournal()));
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.SOURCE_FILE_LOCATION,
				currentFile.getParent());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.BASE_PATH,
				basePath);
			String relativeToBasePath = "";
			if (currentFile.getParent().length() > basePath.length()){
				relativeToBasePath = currentFile.getParent().substring(
					basePath.length());
			}
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.RELATIVE_PATH,
				relativeToBasePath);
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.PRESERVE_BASE_PATH,
				String.valueOf(preserveBasePath));
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.PRESERVE_RELATIVE_PATH,
				String.valueOf(preserveRelativePath));
			logger.debug(handlerPhase, "setting entityName header, value={}",
				entityName);
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.ENTITY_NAME, entityName);
			outputEvent.getHeaders().put(
					ActionEventHeaderConstants.READ_COMPLETE,
					Boolean.FALSE.toString());
			
			getHandlerContext().createSingleItemEventList(outputEvent);
		
			logger.debug(
				handlerPhase,
				"_message=\"handler read data, ready to return\", context.list_size={} total_read={} total_size={} context={} fileSize={}",
				getHandlerContext().getEventList().size(),
				getTotalReadFromJournal(), getTotalSizeFromJournal(),
				getHandlerContext(), getZipFileHandlerJournal()
						.getTotalSize());

			processChannelSubmission(outputEvent);
			
			if(!readAllFromFile() &&
				getZipFileHandlerJournal().getEntryName().equalsIgnoreCase(fileName)){
				fileSize = fileSize - bytesReadCount;
				bytesReadCount = 0;
				return Status.CALLBACK;
			} else if(!getZipFileHandlerJournal().getEntryName().equalsIgnoreCase(fileName)){
				logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"file name mismatch during read file");
				throw new HandlerException("file name is not same as file name in Journal");
			} else{

				if(readEntries == noOfEntry){
					
					logger.debug(handlerPhase, "returning READY, no file need read from the zip");
					logger.debug(handlerPhase,
						"_message=\"handler read data, ready to return\", context.list_size={} total_read={} total_size={} context={} fileSize={}",
						getHandlerContext().getEventList().size(), getTotalReadFromJournal(), getTotalSizeFromJournal(),
						getHandlerContext(), getZipFileHandlerJournal().getTotalSize());
					totalRead = 0;
					getZipFileHandlerJournal().reset();
					readEntries = 0;
					getZipFileHandlerJournal().setReadComplete(true);
					outputEvent.getHeaders().put(
						ActionEventHeaderConstants.READ_COMPLETE,
						Boolean.TRUE.toString());
					return Status.READY;
				}else{
					logger.debug(
						handlerPhase,
						"_message=\"done reading file={}, there might be more files to process, returning CALLBACK\" handler_id={} headers_from_file_handler={}",
						currentFile.getAbsolutePath(), getId(),
						outputEvent.getHeaders());
					readEntries++;
					getZipFileHandlerJournal().setTotalRead(0);
					getZipFileHandlerJournal().setTotalSize(0);
					getZipFileHandlerJournal().setEntryName(null);
					getZipFileHandlerJournal().setSrcFileName(null);
					getZipFileHandlerJournal().setReadComplete(false);
					return Status.CALLBACK;
				}
			}
		}else {
			logger.debug(handlerPhase, "returning READY, no data read from the file");
			return Status.READY;
		}		
	}

	protected File getNextFileToProcess()
			throws FileNotFoundException, RuntimeInfoStoreException, HandlerException {

		logger.debug(handlerPhase, "calling runtime_info store to get next descriptor");
		List<String> availableFiles = getAvailableFiles();
		if (availableFiles == null || availableFiles.isEmpty()) {
			return null;
		}
		String nextDescriptorToProcess = getNextDescriptorToProcess(runtimeInfoStore, entityName, availableFiles,
				fileInputDescriptor);
		logger.debug(handlerPhase, "next_descriptor_to_process={}", nextDescriptorToProcess);
		if (nextDescriptorToProcess != null) {
			boolean addSuccessful = addRuntimeInfo(runtimeInfoStore, entityName, nextDescriptorToProcess);
			if (addSuccessful) {
//				file = new RandomAccessFile(nextDescriptorToProcess, "r");
				currentFile = new File(nextDescriptorToProcess);
				logger.debug(handlerPhase, "absolute_path={} file_name_for_descriptor={}",
						currentFile.getAbsolutePath(), currentFile.getName());
				return currentFile;
			} else {
				throw new HandlerException(
						"unable to add the new RuntimeInfo to the store. check if there is another node processing the same input descriptor.");
			}
		} else {
			return null;
		}
	}

	
	
	private int getNumberOfFileInZip(File currentZipFile) {
		try {
			zipFile = new ZipFile(currentZipFile);
			return zipFile.size();
		} catch (ZipException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	private String fileNameFromZip(Enumeration<? extends ZipEntry> Zentries){
		zipFileEntry = Zentries.nextElement();
		return zipFileEntry.getName();
	}
	
	protected List<String> getAvailableFiles() {
		try {
			logger.debug(handlerPhase, "getting next file list, fileLocation=\"{}\" fileNamePattern=\"{}\"",
					fileLocation, fileNamePattern);
			return fileHelper.getAvailableFiles(fileLocation, fileNamePattern);
		} catch (IllegalArgumentException ex) {
			logger.warn(handlerPhase,
					"_message=\"no file found, make sure fileLocation and fileNamePattern are correct\" fileLocation={} fileNamePattern={}",
					fileLocation, fileNamePattern, ex);
		}
		return null;
	}

	@Override
	public void shutdown() {
		super.shutdown();
		shutdown0();
	}

	private void shutdown0() {
		throw new NotImplementedException();
	}

	private long getTotalReadFromJournal() throws HandlerException {
		HandlerJournal journal = getZipFileHandlerJournal();
		return journal.getTotalRead();
	}

	private long getTotalSizeFromJournal() throws HandlerException {
		HandlerJournal journal = getZipFileHandlerJournal();
		return journal.getTotalSize();
	}
	
	private int getNoOfEntriesFromJournal() throws HandlerException {
		return getZipFileHandlerJournal().getTotalEntries();
	}
	
	private int getReadEntriesFromJournal() throws HandlerException {
		return getZipFileHandlerJournal().getReadEntries();
	}

	private ZipFileHandlerJournal getZipFileHandlerJournal() throws HandlerException {
		ZipFileHandlerJournal journal =	getNonNullJournal(ZipFileHandlerJournal.class);
		return journal;
	}

	private boolean readAllFromFile() throws HandlerException {
		if (getTotalReadFromJournal() == getTotalSizeFromJournal()) {
			return true;
		}
		return false;
	}
	
	private boolean readAllFromZip() throws HandlerException {
		if (getReadEntriesFromJournal() == getNoOfEntriesFromJournal()) {
			return true;
		}
		return false;
	}

	protected String getHandlerPhase() {
		return handlerPhase;
	}

	public String getFileNamePattern() {
		return fileNamePattern;
	}

	public String getEntityName() {
		return entityName;
	}

	public String getBasePath() {
		return basePath;
	}

}
