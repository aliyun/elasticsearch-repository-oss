package org.elasticsearch.aliyun.oss.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.aliyun.oss.service.OssService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.utils.PermissionHelper;

/**
 * An oss blob store for managing oss client write and read blob directly
 * Created by yangkongshi on 2017/11/24.
 */
public class OssBlobStore implements BlobStore {

    private final OssService client;
    private final String bucket;

    public OssBlobStore(String bucket, OssService client) {
        this.client = client;
        this.bucket = bucket;
        if (!doesBucketExist(bucket)) {
            throw new BlobStoreException("bucket does not exist");
        }
    }

    public String getBucket() {
        return this.bucket;
    }

    @Override
    public BlobContainer blobContainer(BlobPath blobPath) {
        return new OssBlobContainer(blobPath, this);
    }

    @Override
    public void delete(BlobPath blobPath) throws IOException {
        DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
        Map<String, BlobMetaData> blobs = listBlobsByPrefix(blobPath.buildAsString(), null);
        List<String> toBeDeletedBlobs = new ArrayList<>();
        Iterator<String> blobNameIterator = blobs.keySet().iterator();
        while (blobNameIterator.hasNext()) {
            String blobName = blobNameIterator.next();
            toBeDeletedBlobs.add(blobPath.buildAsString() + blobName);
            if (toBeDeletedBlobs.size() > DeleteObjectsRequest.DELETE_OBJECTS_ONETIME_LIMIT / 2
                || !blobNameIterator.hasNext()) {
                deleteRequest.setKeys(toBeDeletedBlobs);
                deleteObjects(deleteRequest);
                toBeDeletedBlobs.clear();
            }
        }
    }

    @Override
    public void close() {
        client.shutdown();
    }

    /**
     * Return true if the given bucket exists
     *
     * @param bucketName name of the bucket
     * @return true if the bucket exists, false otherwise
     */
    boolean doesBucketExist(String bucketName) {
        try {
            return doPrivilegedAndRefreshClient(() -> this.client.doesBucketExist(bucketName));
        } catch (IOException e) {
            throw new BlobStoreException("do privileged has failed", e);
        }
    }

    /**
     * List all blobs in the bucket which have a prefix
     *
     * @param prefix prefix of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetaData> listBlobsByPrefix(String keyPath, String prefix) throws IOException {
        MapBuilder<String, BlobMetaData> blobsBuilder = MapBuilder.newMapBuilder();
        String actualPrefix = keyPath + (prefix == null ? StringUtils.EMPTY : prefix);
        String nextMarker = null;
        ObjectListing blobs;
        do {
            blobs = listBlobs(actualPrefix, nextMarker);
            for (OSSObjectSummary summary : blobs.getObjectSummaries()) {
                String blobName = summary.getKey().substring(keyPath.length());
                blobsBuilder.put(blobName, new PlainBlobMetaData(blobName, summary.getSize()));
            }
            nextMarker = blobs.getNextMarker();
        } while (blobs.isTruncated());
        return blobsBuilder.immutableMap();
    }

    /**
     * list blob with privilege check
     *
     * @param actualPrefix actual prefix of the blobs to list
     * @param nextMarker   blobs next marker
     * @return {@link ObjectListing}
     */
    ObjectListing listBlobs(String actualPrefix, String nextMarker) throws IOException {
        return doPrivilegedAndRefreshClient(() -> this.client.listObjects(
            new ListObjectsRequest(bucket).withPrefix(actualPrefix).withMarker(nextMarker)
        ));
    }

    /**
     * Delete Objects
     *
     * @param deleteRequest {@link DeleteObjectsRequest}
     */
    void deleteObjects(DeleteObjectsRequest deleteRequest) throws IOException {
        doPrivilegedAndRefreshClient(() -> this.client.deleteObjects(deleteRequest));
    }

    /**
     * Returns true if the blob exists in the bucket
     *
     * @param blobName name of the blob
     * @return true if the blob exists, false otherwise
     */
    boolean blobExists(String blobName) throws OSSException, ClientException, IOException {
        return doPrivilegedAndRefreshClient(() -> this.client.doesObjectExist(bucket, blobName));
    }

    /**
     * Returns an {@link java.io.InputStream} for a given blob
     *
     * @param blobName name of the blob
     * @return an InputStream
     */
    InputStream readBlob(String blobName) throws OSSException, ClientException, IOException {
        return doPrivilegedAndRefreshClient(() -> this.client.getObject(bucket, blobName).getObjectContent());
    }

    /**
     * Writes a blob in the bucket.
     *
     * @param inputStream content of the blob to be written
     * @param blobSize    expected size of the blob to be written
     */
    void writeBlob(String blobName, InputStream inputStream, long blobSize)
        throws OSSException, ClientException, IOException {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(blobSize);
        doPrivilegedAndRefreshClient(() -> this.client.putObject(bucket, blobName, inputStream, meta));
    }

    /**
     * Deletes a blob in the bucket
     *
     * @param blobName name of the blob
     */
    void deleteBlob(String blobName) throws OSSException, ClientException, IOException {
        doPrivilegedAndRefreshClient(() -> {
            this.client.deleteObject(bucket, blobName);
            return null;
        });
    }

    public void move(String sourceBlobName, String targetBlobName)
        throws OSSException, ClientException, IOException {
        doPrivilegedAndRefreshClient(() -> {
            this.client.copyObject(bucket, sourceBlobName, bucket, targetBlobName);
            return null;
        });
        doPrivilegedAndRefreshClient(() -> {
            this.client.deleteObject(bucket, sourceBlobName);
            return null;
        });
    }

    /**
     * Executes a {@link PrivilegedExceptionAction} with privileges enabled.
     */
    <T> T doPrivilegedAndRefreshClient(PrivilegedExceptionAction<T> operation) throws IOException {
        refreshStsOssClient();
        return PermissionHelper.doPrivileged(operation);
    }

    private void refreshStsOssClient() throws IOException {
        if (this.client.isUseStsOssClient()) {
            this.client.refreshStsOssClient();//refresh token to avoid expired
        }
    }
}
