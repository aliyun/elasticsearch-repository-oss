package org.elasticsearch.plugin.repository.oss;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.aliyun.oss.service.OssClientSettings;
import org.elasticsearch.aliyun.oss.service.OssService;
import org.elasticsearch.aliyun.oss.service.OssServiceImpl;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repository.oss.OssRepository;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A plugin to add a repository type that writes to and from OSS.
 * Created by yangkongshi on 2017/11/24.
 */
public class OssRepositoryPlugin extends Plugin implements RepositoryPlugin {

    static {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            return null;
        });
    }

    protected OssService createStorageService(Settings settings, RepositoryMetaData metadata) {
        return new OssServiceImpl(settings, metadata);
    }

    @Override public Map<String, Repository.Factory> getRepositories(Environment env,
        NamedXContentRegistry namedXContentRegistry) {
        return Collections.singletonMap(OssRepository.TYPE,
            (metadata) -> new OssRepository(metadata, env, namedXContentRegistry,
                createStorageService(env.settings(), metadata)));
    }

    @Override public List<Setting<?>> getSettings() {
        return Arrays.asList(OssClientSettings.ACCESS_KEY_ID, OssClientSettings.SECRET_ACCESS_KEY,
            OssClientSettings.ENDPOINT, OssClientSettings.BUCKET, OssClientSettings.SECURITY_TOKEN,
            OssClientSettings.BASE_PATH, OssClientSettings.COMPRESS, OssClientSettings.CHUNK_SIZE);
    }
}
