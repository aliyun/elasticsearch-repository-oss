package org.elasticsearch.aliyun.oss.service;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import static org.elasticsearch.common.settings.Setting.*;

/**
 * OSS client configuration
 * Created by yangkongshi on 2017/11/27.
 */
public class OssClientSettings {
    private static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.MB);
    private static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.GB);

    public static final Setting<String> ACCESS_KEY_ID =
        Setting.simpleString("access_key_id", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> SECRET_ACCESS_KEY = Setting
        .simpleString("secret_access_key", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> ENDPOINT =
        Setting.simpleString("endpoint", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> SECURITY_TOKEN = Setting
        .simpleString("security_token", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> BUCKET =
        simpleString("bucket", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> BASE_PATH =
        simpleString("base_path", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<Boolean> COMPRESS =
        boolSetting("compress", false, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<ByteSizeValue> CHUNK_SIZE =
        byteSizeSetting("chunk_size", MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE,
            Setting.Property.NodeScope, Setting.Property.Dynamic);
}
