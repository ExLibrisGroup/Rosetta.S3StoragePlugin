package com.exlibris.dps.repository.plugin.storage.s3;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.exlibris.core.infra.common.exceptions.logging.ExLogger;
import com.exlibris.core.sdk.storage.containers.StoredEntityMetaData;
import com.exlibris.core.sdk.storage.handler.AbstractStorageHandler;
import com.exlibris.digitool.common.storage.Fixity;

public class S3StoragePlugin extends AbstractStorageHandler {

	private final long waitingMsStep = 500;
	private AmazonS3 s3Client;
	private S3Object object;
	private static final ExLogger log = ExLogger.getExLogger(S3StoragePlugin.class);

	/*
	 * Delete file
	 *
	 * (non-Javadoc)
	 * @see com.exlibris.digitool.common.storage.StoragePlugin#getFullFilePath(java.lang.String)
	 */
	@Override
	public boolean deleteEntity(String entityIdentifier) {
		AmazonS3 s3 = new AmazonS3Client(new BasicAWSCredentials(getAccessKeyId(), getSecretKey()));
		s3.deleteObject(getBucketName(), entityIdentifier);
		return true;
	}

	public List<Fixity> getFixityValues(String entityIdentifier) {
		AmazonS3 s3 = new AmazonS3Client(new BasicAWSCredentials(getAccessKeyId(), getSecretKey()));
		ObjectMetadata metadata = s3.getObjectMetadata(getBucketName(), entityIdentifier);
		String etagValue = metadata.getETag();
		List<Fixity> fixities = new ArrayList<Fixity>();
		if(!ServiceUtils.isMultipartUploadETag(etagValue)){
			/*
			 * Etag doesn't represent MD5 on MFU
			 */
			fixities.add(new Fixity("MD5", etagValue));
		}
		return fixities;
	}

	@Override
	public InputStream retrieveEntity(String entityIdentifier) {
		BasicAWSCredentials myCredentials = new BasicAWSCredentials(getAccessKeyId(), getSecretKey());
		s3Client = new AmazonS3Client(myCredentials);
		S3Object object = s3Client.getObject(new GetObjectRequest(getBucketName(), entityIdentifier));
		/* NOTE: close() MUST BE CALLED to close connection with Amazon.
		 * There are only 50 connections available for the same user-bucket
		 */
		return object.getObjectContent();
	}

	@Override
	public String storeEntity(InputStream is,
			StoredEntityMetaData storedEntityMetadata) {

		BasicAWSCredentials myCredentials = new BasicAWSCredentials(getAccessKeyId(), getSecretKey());
		TransferManager tm = new TransferManager(myCredentials);

		//tm.getConfiguration().setMultipartUploadThreshold(5 * 1024 * Constants.MB); //decided to use default thresholds

		// unique id will have the same naming convention as it used to be in Rosetta NFS
		String uniqueId = createFileName(storedEntityMetadata);
		ObjectMetadata metadata = createAmazonMetadata(storedEntityMetadata);
		File nfsFile = null;
		Upload upload = null;

		try {
			nfsFile = new File(storedEntityMetadata.getCurrentFilePath());
		} catch (NullPointerException e) {
			//ignore. If failed to find NFS file path, upload file via InputStream
		}

		long start = System.currentTimeMillis();
		if(nfsFile == null || !nfsFile.exists()) {
			upload = tm.upload(getBucketName(), uniqueId, new BufferedInputStream(is), metadata);
		} else {
			//use the file upload method because it is much faster.
			upload = tm.upload(getBucketName(), uniqueId, nfsFile);
		}

		try {
			monitorUploadingProcess(storedEntityMetadata, upload);

		} catch (AmazonClientException amazonClientException) {
			log.error("Unable to upload file, upload was aborted.", amazonClientException);
			throw amazonClientException;
		}
		catch (InterruptedException e) {
			log.error("Amazon upload was interrupted", e);
		}
		finally{
			tm.shutdownNow();
		}
		// If succeeded - return the unique id
		if(handleUploadSucceeded(upload, uniqueId, storedEntityMetadata, start)){
			return uniqueId;
		}

		// If failed - delete an entity if was created
		deleteEntity(uniqueId);
		long min = (System.currentTimeMillis() - start)/60000;
		log.error("Failed to upload file "+uniqueId+" to Amazon "+upload.isDone()+" "+upload.getState()+" within "+min+" min");
		throw new RuntimeException("Failed to upload file "+uniqueId+" to Amazon ");
	}


	/*
	 * When sending InputStream to Amazon - contentLength should be specified (for mupltiparts upload)
	 *
	 * There is no need to set contentMD5, AmazonS3Client will calculate it on the fly and validate it
	 * with the returned ETag from the object upload. See AmazonS3Client.uploadPart for more details
	 *
	 * http://aws.amazon.com/releasenotes/Java/9726474311325478
	 *
	 * When uploading Multipart Uploads using the Amazon S3 client, or the high level TransferManager class,
	 * the SDK calculates an MD5 checksum on the fly and compares it with Amazon S3's checksum to make sure your data was correctly uploaded to Amazon S3.

	 */
	private ObjectMetadata createAmazonMetadata(
 			StoredEntityMetaData storedEntityMetadata) {
		ObjectMetadata metadata = new ObjectMetadata();

		// set file size that needed to be split to parts for uploading
		metadata.setContentLength(storedEntityMetadata.getSizeInBytes());
		return metadata;
	}

	/*
	 * upload.waitForCompletion()  will block and wait for the upload to finish (unlimited in time).
	 * So, instead - loop sleeps until file uploaded or until timeout exceeded
	 */
	private void monitorUploadingProcess(StoredEntityMetaData storedEntityMetadata, Upload upload) throws InterruptedException {
		long waitedForMs = 0;
		if(getMaxWaitingTime() > 0){
			if(storedEntityMetadata.getSizeInBytes() > 0){
				log.debug("Transfering "+storedEntityMetadata.getSizeInBytes()/1024/1024+" MB");
			}
			// Wait for upload to complete...
			while (!upload.isDone() && waitedForMs < getMaxWaitingTime()) {
				log.debug("Transfer: " + upload.getDescription());
				log.debug("  - State: " + upload.getState());
				log.debug("  - Progress in bytes: " + upload.getProgress().getBytesTransfered());
				waitedForMs += waitingMsStep;
				Thread.sleep(waitingMsStep);
				//every five minutes
				if(waitedForMs % 300000 == 0) {
					log.info("################# Transfer: " + upload.getDescription());
					log.info("#################  - State: " + upload.getState());
					log.info("#################  - Progress in bytes: " + upload.getProgress().getBytesTransfered());
				}
			}
		}
		else{
			upload.waitForCompletion();
		}

	}

	/*
	 * Check the Upload status. If fixity algorithm doesn't meet 1 of our supported - delete the stored entity
	 */
	private boolean handleUploadSucceeded(Upload upload, String uniqueId, StoredEntityMetaData storedEntityMetadata, long start) {
		if(upload.isDone() && upload.getState() == TransferState.Completed){
			List<Fixity> fixities = getFixityValues(uniqueId);
			if(fixities == null || fixities.isEmpty()){
				long min = (System.currentTimeMillis() - start)/60000;
				log.info("File uploaded "+uniqueId+" to Amazon "+upload.isDone()+" "+upload.getState()+" within "+min+" min");
				return true;
			}

			else {
				String md5NewValue = fixities.get(0).getValue();
				//validate stored entity fixity with the original one
				String md5OriginalValue = storedEntityMetadata.getFixityByType(Fixity.FixityAlgorithm.MD5.toString());
				if(md5NewValue == null || md5OriginalValue == null ||
						md5OriginalValue != null && md5NewValue != null && md5NewValue.equalsIgnoreCase(md5OriginalValue)){
					long min = (System.currentTimeMillis() - start)/60000;
					log.info("File uploaded "+uniqueId+" to Amazon "+upload.isDone()+" "+upload.getState()+" within "+min+" min");
					return true;
				}else {
					return false;
				}
			}
		}
		return false;
	}

	@Override
	public boolean checkFixity(List<Fixity> fixities,
			String storedEntityIdentifier) throws Exception {
		for(Fixity fixity: fixities) {
			fixity.setResult(true);
		}
		return true;
	}

	/**
	 *
	 */
	@Override
	public String getLocalFilePath(String storedEntityIdentifier) {
		String newFilePath = getTempStorageDirectory() + storedEntityIdentifier;
		File checkExists = new File(newFilePath);
		if(! checkExists.exists()){
			AmazonS3 s3Client = new AmazonS3Client(new BasicAWSCredentials(getAccessKeyId(), getSecretKey()));
	        S3Object obj = s3Client.getObject(getBucketName(), storedEntityIdentifier);
	        // also checks checksum after download if not MPU
			ServiceUtils.downloadObjectToFile(obj, new File(newFilePath));
		}
		return newFilePath;
	}

	@Override
	public String getFullFilePath(String storedEntityIdentifier) {
		AmazonS3 s3Client = new AmazonS3Client(new BasicAWSCredentials(getAccessKeyId(), getSecretKey()));
        S3Object obj = s3Client.getObject(getBucketName(), storedEntityIdentifier);
        return obj.getObjectContent().getHttpRequest().getURI().toASCIIString();
	}

	private String getAccessKeyId() {
		return parameters.get("accessKeyId");
	}
	private String getSecretKey() {
		return parameters.get("secretKey");
	}
	private String getBucketName() {
		return parameters.get("bucketName");
	}
	private long getMaxWaitingTime() {
		return Long.parseLong(parameters.get("maxWaitingTime"));
	}

	@Override
	public byte[] retrieveEntityByRange(String storedEntityIdentifier, long start, long end) throws Exception{
		BasicAWSCredentials myCredentials = new BasicAWSCredentials(getAccessKeyId(), getSecretKey());
		s3Client = new AmazonS3Client(myCredentials);
		GetObjectRequest objectRequest = new GetObjectRequest(getBucketName(), storedEntityIdentifier);
		objectRequest.setRange(start, end);
		object = s3Client.getObject(objectRequest);
		if(object == null) {
			log.error("Amazon could not retrieve the requested range for the file " + storedEntityIdentifier);
			throw new Exception("Amazon could not retrieve the requested range for the file " + storedEntityIdentifier);
		}
		InputStream is = object.getObjectContent();
		byte[] bytes = null;
		try{
			bytes = IOUtils.toByteArray(is);
		} catch(Exception e) {
			log.error("Exception thrown while copying InputStream to byte array", e);
		} finally {
			//NOTE: close() MUST BE CALLED to close inputStream.
			//There are only 50 connections available for the same user-bucket
			is.close();
		}
		return bytes;
	}

	public boolean isAvailable() {

		try {
			AmazonS3 s3 = new AmazonS3Client(new BasicAWSCredentials(getAccessKeyId(), getSecretKey()));
			s3.listBuckets();
		} catch(Exception e) {
			log.error("failed to access amazon storage acount.");
			return false;
		}
		return true;
	}

}
