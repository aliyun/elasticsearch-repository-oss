package org.elasticsearch.aliyun.oss.blobstore;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.*;
import org.elasticsearch.aliyun.oss.service.OssService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yangkongshi on 2017/11/28.
 */
public class MockOssService extends AbstractComponent implements OssService {

    protected final Map<String, OSSObject> blobs = new ConcurrentHashMap<>();

    public MockOssService() {
        super(Settings.EMPTY);
    }

    @Override public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest)
        throws OSSException, ClientException {
        for (String key : deleteObjectsRequest.getKeys()) {
            blobs.remove(key);
        }
        return null;
    }

    @Override public boolean doesObjectExist(String bucketName, String key)
        throws OSSException, ClientException {
        return blobs.containsKey(key);
    }

    @Override public boolean doesBucketExist(String bucketName)
        throws OSSException, ClientException {
        return true;
    }

    @Override public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
        throws OSSException, ClientException {
        ObjectListing objectListing = new ObjectListing();
        objectListing.setTruncated(false);
        String prefix = listObjectsRequest.getPrefix();
        blobs.forEach((String blobName, OSSObject ossObject) -> {
            if (!Strings.hasLength(prefix) || startsWithIgnoreCase(blobName, prefix)) {
                OSSObjectSummary summary = new OSSObjectSummary();
                summary.setKey(blobName);
                summary.setSize(ossObject.getObjectMetadata().getContentLength());
                objectListing.addObjectSummary(summary);
            }
        });
        return objectListing;
    }

    @Override public OSSObject getObject(String bucketName, String key)
        throws OSSException, ClientException, IOException {
        OSSObject ossObject = new OSSObject();
        synchronized (this) {
            OSSObject oldObject = blobs.get(key);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Streams.copy(oldObject.getObjectContent(), outputStream);
            oldObject.setObjectContent(new ByteArrayInputStream(outputStream.toByteArray()));
            ossObject.setObjectContent(new ByteArrayInputStream(outputStream.toByteArray()));
        }
        return ossObject;
    }

    @Override public PutObjectResult putObject(String bucketName, String key, InputStream input,
        ObjectMetadata metadata) throws OSSException, ClientException, IOException {
        synchronized (this) {
            OSSObject ossObject = new OSSObject();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Streams.copy(input, outputStream);
            ossObject.setObjectContent(new ByteArrayInputStream(outputStream.toByteArray()));
            ossObject.setObjectMetadata(metadata);
            blobs.put(key, ossObject);
        }
        return null;
    }

    @Override public void deleteObject(String bucketName, String key)
        throws OSSException, ClientException {
        blobs.remove(key);
    }

    @Override public CopyObjectResult copyObject(String sourceBucketName, String sourceKey,
        String destinationBucketName, String destinationKey) throws OSSException, ClientException {
        OSSObject sourceOssObject = blobs.get(sourceKey);
        blobs.put(destinationKey, sourceOssObject);
        return null;
    }

    @Override public void shutdown() {
        blobs.clear();
    }

    /**
     * Test if the given String starts with the specified prefix,
     * ignoring upper/lower case.
     *
     * @param str    the String to check
     * @param prefix the prefix to look for
     * @see java.lang.String#startsWith
     */
    public static boolean startsWithIgnoreCase(String str, String prefix) {
        if (str == null || prefix == null) {
            return false;
        }
        if (str.startsWith(prefix)) {
            return true;
        }
        if (str.length() < prefix.length()) {
            return false;
        }
        String lcStr = str.substring(0, prefix.length()).toLowerCase(Locale.ROOT);
        String lcPrefix = prefix.toLowerCase(Locale.ROOT);
        return lcStr.equals(lcPrefix);
    }


}
