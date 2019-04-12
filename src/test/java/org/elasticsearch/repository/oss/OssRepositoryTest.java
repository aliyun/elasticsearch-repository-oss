package org.elasticsearch.repository.oss;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.aliyun.oss.blobstore.MockOssService;
import org.elasticsearch.aliyun.oss.service.OssClientSettings;
import org.elasticsearch.aliyun.oss.service.OssService;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.plugin.repository.oss.OssRepositoryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Created by yangkongshi on 2017/11/28.
 */
public class OssRepositoryTest extends ESBlobStoreRepositoryIntegTestCase {
    private static final String BUCKET = "oss-repository-test";
    private static final OssService client = new MockOssService();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockOssRepositoryPlugin.class);
    }

    @Override
    protected void createTestRepository(String name) {
        assertAcked(
            client().admin().cluster().preparePutRepository(name).setType(OssRepository.TYPE)
                .setSettings(Settings.builder().put(OssClientSettings.BUCKET.getKey(), BUCKET)
                    .put(OssClientSettings.BASE_PATH.getKey(), StringUtils.EMPTY)
                    .put(OssClientSettings.ACCESS_KEY_ID.getKey(), "test_access_key_id")
                    .put(OssClientSettings.SECRET_ACCESS_KEY.getKey(), "test_secret_access_key")
                    .put(OssClientSettings.ENDPOINT.getKey(), "test_endpoint")
                    .put(OssClientSettings.COMPRESS.getKey(), randomBoolean())
                    .put(OssClientSettings.CHUNK_SIZE.getKey(), randomIntBetween(100, 1000),
                        ByteSizeUnit.MB)));
    }

    public static class MockOssRepositoryPlugin extends OssRepositoryPlugin {
        @Override
        protected OssService createStorageService(Settings settings, RepositoryMetaData metadata) {
            return client;
        }
    }

}
