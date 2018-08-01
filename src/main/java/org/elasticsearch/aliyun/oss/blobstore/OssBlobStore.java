package org.elasticsearch.aliyun.oss.blobstore;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.*;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.aliyun.oss.service.OssService;
import org.elasticsearch.common.blobstore.*;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An oss blob store for managing oss client write and read blob directly
 * Created by yangkongshi on 2017/11/24.
 */
public class OssBlobStore extends AbstractComponent implements BlobStore {

    private final OssService client;
    private final String bucket;

    public OssBlobStore(Settings settings, String bucket, OssService client) {
        super(settings);
        this.client = client;
        this.bucket = bucket;
        if (!doesBucketExist(bucket)) {
            throw new BlobStoreException("Bucket [" + bucket + "] does not exist");
        }
    }

    public String getBucket() {
        return this.bucket;
    }

    @Override public BlobContainer blobContainer(BlobPath blobPath) {
        return new OssBlobContainer(blobPath, this);
    }

    @Override public void delete(BlobPath blobPath) throws IOException {
        doPrivileged(() -> {
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
                    this.client.deleteObjects(deleteRequest);
                    toBeDeletedBlobs.clear();
                }
            }
            return null;
        });
    }

    @Override public void close() throws IOException {
        client.shutdown();
    }


    /**
     * Return true if the given bucket exists
     *
     * @param bucketName name of the bucket
     * @return true if the bucket exists, false otherwise
     */
    boolean doesBucketExist(String bucketName) {
        return this.client.doesBucketExist(bucketName);
    }



    /**
     * List all blobs in the bucket which have a prefix
     *
     * @param prefix prefix of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetaData> listBlobsByPrefix(String keyPath, String prefix) throws IOException {
        return doPrivileged(() -> {
            MapBuilder<String, BlobMetaData> blobsBuilder = MapBuilder.newMapBuilder();
            String actualPrefix = keyPath + (prefix == null ? StringUtils.EMPTY : prefix);
            String nextMarker = null;
            ObjectListing blobs;
            do {
                blobs = this.client.listObjects(
                    new ListObjectsRequest(bucket).withPrefix(actualPrefix).withMarker(nextMarker));
                for (OSSObjectSummary summary : blobs.getObjectSummaries()) {
                    String blobName = summary.getKey().substring(keyPath.length());
                    blobsBuilder.put(blobName, new PlainBlobMetaData(blobName, summary.getSize()));
                }
                nextMarker = blobs.getNextMarker();
            } while (blobs.isTruncated());
            return blobsBuilder.immutableMap();
        });
    }


    /**
     * Returns true if the blob exists in the bucket
     *
     * @param blobName name of the blob
     * @return true if the blob exists, false otherwise
     */
    boolean blobExists(String blobName) throws OSSException, ClientException, IOException {
        return doPrivileged(() -> this.client.doesObjectExist(bucket, blobName));
    }

    /**
     * Returns an {@link java.io.InputStream} for a given blob
     *
     * @param blobName name of the blob
     * @return an InputStream
     */
    InputStream readBlob(String blobName) throws OSSException, ClientException, IOException {
        return doPrivileged(() -> this.client.getObject(bucket, blobName).getObjectContent());
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
        doPrivileged(() -> this.client.putObject(bucket, blobName, inputStream, meta));
    }

    /**
     * Deletes a blob in the bucket
     *
     * @param blobName name of the blob
     */
    void deleteBlob(String blobName) throws OSSException, ClientException, IOException {
        doPrivileged(() -> {
            this.client.deleteObject(bucket, blobName);
            return null;
        });
    }

    public void move(String sourceBlobName, String targetBlobName)
        throws OSSException, ClientException, IOException {
        doPrivileged(() -> {
            this.client.copyObject(bucket, sourceBlobName, bucket, targetBlobName);
            this.client.deleteObject(bucket, sourceBlobName);
            return null;
        });
    }

    /**
     * Executes a {@link PrivilegedExceptionAction} with privileges enabled.
     */
    <T> T doPrivileged(PrivilegedExceptionAction<T> operation) throws IOException {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) operation::run);
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getException();
        }
    }
}
