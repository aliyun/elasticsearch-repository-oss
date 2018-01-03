package org.elasticsearch.aliyun.oss.blobstore;

import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.ESBlobStoreContainerTestCase;

import java.io.IOException;
import java.util.Locale;

/**
 * Created by yangkongshi on 2017/11/28.
 */
public class OssBlobContainerTest extends ESBlobStoreContainerTestCase {
    @Override protected BlobStore newBlobStore() throws IOException {
        String bucket = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        MockOssService client = new MockOssService();
        return new OssBlobStore(Settings.EMPTY, bucket, client);
    }
}
