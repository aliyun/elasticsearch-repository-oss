package org.elasticsearch.aliyun.oss.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectResult;
import okhttp3.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.aliyun.oss.service.exception.CreateStsOssClientException;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.repository.oss.OssRepository;
import org.elasticsearch.utils.DateHelper;
import org.elasticsearch.utils.HttpClientHelper;

import static java.lang.Thread.sleep;

/**
 * @author hanqing.zhq@alibaba-inc.com
 * @date 2018/6/26
 */
public class OssStorageClient {
    private final Logger logger = Loggers.getLogger(OssStorageClient.class);

    private RepositoryMetaData metadata;
    private OSSClient client;
    private Date stsTokenExpiration;
    private String ECS_METADATA_SERVICE = "http://100.100.100.200/latest/meta-data/ram/security-credentials/";
    private final int IN_TOKEN_EXPIRED_MS = 5000;
    private final String ACCESS_KEY_ID = "AccessKeyId";
    private final String ACCESS_KEY_SECRET = "AccessKeySecret";
    private final String SECURITY_TOKEN = "SecurityToken";
    private final int REFRESH_RETRY_COUNT = 3;
    private boolean isStsOssClient;
    private ReadWriteLock readWriteLock;

    private final String EXPIRATION = "Expiration";

    public OssStorageClient(RepositoryMetaData metadata) throws CreateStsOssClientException {
        this.metadata = metadata;
        if (StringUtils.isNotEmpty(OssClientSettings.ECS_RAM_ROLE.get(metadata.settings()).toString())) {
            isStsOssClient = true;
        } else {
            isStsOssClient = false;
        }
        readWriteLock = new ReentrantReadWriteLock();
        client = createClient(metadata);

    }

    public boolean isStsOssClient() {
        return isStsOssClient;
    }

    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.deleteObjects(deleteObjectsRequest);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.deleteObjects(deleteObjectsRequest);
        }
    }

    public boolean doesObjectExist(String bucketName, String key)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.doesObjectExist(bucketName, key);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.doesObjectExist(bucketName, key);
        }
    }

    public boolean doesBucketExist(String bucketName)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.doesBucketExist(bucketName);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.doesBucketExist(bucketName);
        }
    }

    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.listObjects(listObjectsRequest);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.listObjects(listObjectsRequest);
        }
    }

    public OSSObject getObject(String bucketName, String key)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.getObject(bucketName, key);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.getObject(bucketName, key);
        }
    }

    public PutObjectResult putObject(String bucketName, String key, InputStream input,
        ObjectMetadata metadata) throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.putObject(bucketName, key, input, metadata);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.putObject(bucketName, key, input, metadata);
        }
    }

    public void deleteObject(String bucketName, String key)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                this.client.deleteObject(bucketName, key);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            this.client.deleteObject(bucketName, key);
        }

    }

    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey,
        String destinationBucketName, String destinationKey) throws OSSException, ClientException {

        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client
                    .copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client
                .copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        }
    }

    public void refreshStsOssClient() throws CreateStsOssClientException {
        int retryCount = 0;
        while (isStsTokenExpired() || isTokenWillExpired()) {
            retryCount++;
            if (retryCount > REFRESH_RETRY_COUNT) {
                logger.error("Can't get valid token after retry {} times", REFRESH_RETRY_COUNT);
                throw new CreateStsOssClientException(
                    "Can't get valid token after retry " + REFRESH_RETRY_COUNT + " times");
            }
            this.client = createStsOssClient(this.metadata);
            try {
                if (isStsTokenExpired() || isTokenWillExpired()) {
                    sleep(IN_TOKEN_EXPIRED_MS * 2);
                }
            } catch (InterruptedException e) {
                logger.error("refresh sleep exception", e);
                throw new CreateStsOssClientException(e);
            }
        }
    }

    public void shutdown() {
        if (isStsOssClient) {
            readWriteLock.writeLock().lock();
            try {
                if (null != this.client) {
                    this.client.shutdown();
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } else {
            if (null != this.client) {
                this.client.shutdown();
            }
        }
    }

    private boolean isStsTokenExpired() {
        boolean expired = true;
        Date now = new Date();
        if (null != stsTokenExpiration) {
            if (stsTokenExpiration.after(now)) {
                expired = false;
            }
        }
        return expired;
    }

    private boolean isTokenWillExpired() {
        boolean in = true;
        Date now = new Date();
        long millisecond = stsTokenExpiration.getTime() - now.getTime();
        if (millisecond >= IN_TOKEN_EXPIRED_MS) {
            in = false;
        }
        return in;
    }

    private OSSClient createClient(RepositoryMetaData repositoryMetaData) throws CreateStsOssClientException {
        OSSClient client;

        String ecsRamRole = OssClientSettings.ECS_RAM_ROLE.get(repositoryMetaData.settings()).toString();
        String stsToken = OssClientSettings.SECURITY_TOKEN.get(repositoryMetaData.settings()).toString();
        /*
         * If ecsRamRole exist
         * means use ECS metadata service to get ststoken for auto snapshot.
         * */
        if (StringUtils.isNotEmpty(ecsRamRole.toString())) {
            client = createStsOssClient(repositoryMetaData);
        } else if (StringUtils.isNotEmpty(stsToken)) {
            //no used still now.
            client = createAKStsTokenClient(repositoryMetaData);
        } else {
            client = createAKOssClient(repositoryMetaData);
        }
        return client;
    }

    private OSSClient createAKOssClient(RepositoryMetaData repositoryMetaData) {
        SecureString accessKeyId =
            OssRepository.getSetting(OssClientSettings.ACCESS_KEY_ID, repositoryMetaData);
        SecureString secretAccessKey =
            OssRepository.getSetting(OssClientSettings.SECRET_ACCESS_KEY, repositoryMetaData);
        String endpoint = OssRepository.getSetting(OssClientSettings.ENDPOINT, repositoryMetaData);
        return new OSSClient(endpoint, accessKeyId.toString(), secretAccessKey.toString());
    }

    private OSSClient createAKStsTokenClient(RepositoryMetaData repositoryMetaData) {
        SecureString securityToken = OssClientSettings.SECURITY_TOKEN.get(repositoryMetaData.settings());
        SecureString accessKeyId =
            OssRepository.getSetting(OssClientSettings.ACCESS_KEY_ID, repositoryMetaData);
        SecureString secretAccessKey =
            OssRepository.getSetting(OssClientSettings.SECRET_ACCESS_KEY, repositoryMetaData);
        String endpoint = OssRepository.getSetting(OssClientSettings.ENDPOINT, repositoryMetaData);
        return new OSSClient(endpoint, accessKeyId.toString(), secretAccessKey.toString(),
            securityToken.toString());
    }

    private synchronized OSSClient createStsOssClient(RepositoryMetaData repositoryMetaData)
        throws CreateStsOssClientException {
        if (isStsTokenExpired() || isTokenWillExpired()) {
            try {
                if (null == repositoryMetaData) {
                    throw new IOException("repositoryMetaData is null");
                }
                String ecsRamRole = OssClientSettings.ECS_RAM_ROLE.get(repositoryMetaData.settings()).toString();
                String endpoint = OssRepository.getSetting(OssClientSettings.ENDPOINT, repositoryMetaData);

                String fullECSMetaDataServiceUrl = ECS_METADATA_SERVICE + ecsRamRole;
                Response response = HttpClientHelper.httpRequest(fullECSMetaDataServiceUrl);
                if (!response.isSuccessful()) {
                    throw new IOException("ECS meta service server error");
                }
                String jsonStringResponse = response.body().string();
                JSONObject jsonObjectResponse = JSON.parseObject(jsonStringResponse);
                String accessKeyId = jsonObjectResponse.getString(ACCESS_KEY_ID);
                String accessKeySecret = jsonObjectResponse.getString(ACCESS_KEY_SECRET);
                String securityToken = jsonObjectResponse.getString(SECURITY_TOKEN);
                stsTokenExpiration = DateHelper.convertStringToDate(jsonObjectResponse.getString(EXPIRATION));
                try {
                    readWriteLock.writeLock().lock();
                    if (null != this.client) {
                        this.client.shutdown();
                    }
                    this.client = new OSSClient(endpoint, accessKeyId, accessKeySecret, securityToken);
                } finally {
                    readWriteLock.writeLock().unlock();
                }
                response.close();
            } catch (IOException e) {
                logger.error("create stsOssClient exception", e);
                throw new CreateStsOssClientException(e);
            }
            return this.client;
        } else {
            return this.client;
        }
    }
}
