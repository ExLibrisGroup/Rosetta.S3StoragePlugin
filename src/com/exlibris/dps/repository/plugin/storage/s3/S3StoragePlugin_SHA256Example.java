package com.exlibris.dps.repository.plugin.storage.s3;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.BinaryUtils;

import com.exlibris.core.infra.common.exceptions.logging.ExLogger;
import com.exlibris.core.infra.common.util.Checksummer;
import com.exlibris.core.sdk.storage.containers.StoredEntityMetaData;
import com.exlibris.core.sdk.storage.handler.AbstractStorageHandler;
import com.exlibris.digitool.common.storage.Fixity;

public class S3StoragePlugin_SHA256Example extends AbstractStorageHandler {

	private static String AMAZON_URL = "https://s3.amazonaws.com";
	private static final ExLogger log = ExLogger.getExLogger(S3StoragePlugin_SHA256Example.class);

	@Override
	public boolean deleteEntity(String entityIdentifier) {

		S3Client s3Client = getS3Client();

		try {

			DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
														.bucket(getBucketName())
														.key(entityIdentifier)
														.build();

			s3Client.deleteObject(deleteObjectRequest);

		} catch (Exception e) {
			log.error("Failed to delete object " + entityIdentifier, e);
			return false;
		} finally {
			s3Client.close();
		}

		return true;
	}



	@Override
	public InputStream retrieveEntity(String entityIdentifier) {

		S3Client s3Client = getS3Client();

		try {

			GetObjectRequest getObjectRequest = GetObjectRequest.builder()
			        .bucket(getBucketName())
			        .key(entityIdentifier)
			        .build();

			return IOUtils.toBufferedInputStream(s3Client.getObject(getObjectRequest));

		} catch (Exception  e) {

			log.error("Failed to retrieve entity " + entityIdentifier, e);
			return null;

		} finally {

			s3Client.close();

		}

	}

	@Override
	public String storeEntity(InputStream is,
			StoredEntityMetaData storedEntityMetadata) {

		S3Client s3Client = getS3Client();

		try {

			// unique id will have the same naming convention as it used to be in Rosetta NFS
			String uniqueId = createFileName(storedEntityMetadata);

			String md5 = storedEntityMetadata.getFixityByType(Fixity.FixityAlgorithm.MD5.toString());
			if (StringUtils.isNotBlank(md5)) {
				md5 = BinaryUtils.toBase64(BinaryUtils.fromHex(md5));
			}

			PutObjectRequest putObjectRequest = PutObjectRequest.builder()
			        .bucket(getBucketName())
			        .key(uniqueId)
			        .contentMD5(md5)
			        .build();

			File nfsFile = null;
			if (StringUtils.isNotBlank(storedEntityMetadata.getCurrentFilePath())) {
				nfsFile = new File(storedEntityMetadata.getCurrentFilePath());
			}

			if (nfsFile == null || !nfsFile.exists()) {
				// If failed to find NFS file path, upload file via InputStream
				s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(is, storedEntityMetadata.getSizeInBytes()));
			} else {
				// use the file upload method because it is much faster.
				s3Client.putObject(putObjectRequest,nfsFile.toPath());
			}

			return uniqueId;

		} catch (AwsServiceException | SdkClientException e) {
			log.error("Failed to upload object", e);
			return null;
		} finally {
			s3Client.close();
		}

	}


	@Override
	public boolean checkFixity(List<Fixity> fixities, String storedEntityIdentifier) throws Exception {
		boolean result = true;
		for(Fixity fixity: fixities) {
			String currentValue = fixity.getValue();
			String awsValue = "";
			currentValue = fixity.getValue();
			if (Fixity.FixityAlgorithm.MD5.toString().equals(fixity.getAlgorithm())) {
				awsValue = getObjectMd5(storedEntityIdentifier);
				result = currentValue == null || StringUtils.equals(currentValue, awsValue);
				fixity.setResult(result);
				fixity.setValue(awsValue);
			} 
			else if (Fixity.FixityAlgorithm.SHA256.toString().equals(fixity.getAlgorithm())){
				awsValue = getObjectSHA256(storedEntityIdentifier);
				result = currentValue == null || StringUtils.equals(currentValue, awsValue);
				fixity.setResult(result);
				fixity.setValue(awsValue);
			}
			else{
				fixity.setResult(true);
			}
			
		}
		return result;
	}



	private String getObjectSHA256(String storedEntityIdentifier) throws IOException {
		String localFilePath= getLocalFilePath(storedEntityIdentifier);
		File file = new File(localFilePath);
		Checksummer checksum = new Checksummer(file, false, false, false, true);
		String value = checksum.getSHA256();
		FileUtils.deleteQuietly(file);
		return value;
	}
	
	private String getObjectMd5(String entityIdentifier) {

		S3Client s3Client = getS3Client();

		try {

			GetObjectRequest getObjectRequest = GetObjectRequest.builder()
			        .bucket(getBucketName())
			        .key(entityIdentifier)
			        .build();

			String eTag = s3Client.getObject(getObjectRequest).response().eTag();
			return StringUtils.strip(eTag, "\"");

		} finally {
			s3Client.close();
		}

	}


	@Override
	public String getLocalFilePath(String storedEntityIdentifier) {
		String localFilePath = getTempStorageDirectory() + storedEntityIdentifier;
		File localFile = new File(localFilePath);

		if (!localFile.exists()) {

			localFile.getParentFile().mkdir();
			S3Client s3Client = getS3Client();
			try {

				GetObjectRequest getObjectRequest = GetObjectRequest.builder()
				        .bucket(getBucketName())
				        .key(storedEntityIdentifier)
				        .build();

				s3Client.getObject(getObjectRequest, localFile.toPath());

			} finally {
				s3Client.close();
			}

		} else {
			localFile.setLastModified(System.currentTimeMillis());
		}
		return localFilePath;
	}

	@Override
	public String getFullFilePath(String storedEntityIdentifier) {

		S3Client s3Client = getS3Client();

		try {

			GetUrlRequest getUrlRequest = GetUrlRequest.builder()
					.endpoint(getEndpoint())
			        .bucket(getBucketName())
			        .key(storedEntityIdentifier)
			        .build();


			return s3Client.utilities().getUrl(getUrlRequest).toExternalForm();

		} finally {
			s3Client.close();
		}

	}

	@Override
	public byte[] retrieveEntityByRange(String storedEntityIdentifier, long start, long end) throws Exception {

		S3Client s3Client = getS3Client();

		try {

			String range = "bytes=" + start + "-" + end;

			GetObjectRequest getObjectRequest = GetObjectRequest.builder()
			        .bucket(getBucketName())
			        .key(storedEntityIdentifier)
			        .range(range)
			        .build();


			return s3Client.getObjectAsBytes(getObjectRequest).asByteArray();

		} catch (Exception e) {
			throw e;
		} finally {
			s3Client.close();

		}

	}

	public boolean isAvailable() {
		S3Client s3Client = getS3Client();
		try {
			s3Client.listBuckets();
		} catch(Exception e) {
			log.error("failed to access amazon storage acount.", e);
			return false;
		} finally {
			s3Client.close();
		}
		return true;
	}

	private S3Client getS3Client(){

		S3Configuration s3Configuration = S3Configuration.builder()
				                .pathStyleAccessEnabled(true)
				                .build();

		ClientOverrideConfiguration clientConfiguration = ClientOverrideConfiguration.builder()
													.putAdvancedOption(SdkAdvancedClientOption.SIGNER , AwsS3V4Signer.create())
													.apiCallTimeout(Duration.ofMillis(getMaxWaitingTime()))
													.build();

		StaticCredentialsProvider credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(getAccessKeyId(), getSecretKey()));

		S3Client s3Client = S3Client.builder()
				.overrideConfiguration(clientConfiguration)
				.region(getRegion())
				.endpointOverride(getEndpoint())
				.serviceConfiguration(s3Configuration)
				.credentialsProvider(credentials)
				.build();

		return s3Client;

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
	private Region getRegion() {
		return parameters.get("region") == null ? Region.US_EAST_1 : Region.of(parameters.get("region"));
	}
	private URI getEndpoint() {
		String endpoint = parameters.get("endpoint") == null ? AMAZON_URL : parameters.get("endpoint");
		return URI.create(endpoint);
	}
	private long getMaxWaitingTime() {
		return Long.parseLong(parameters.get("maxWaitingTime"));
	}

}
