package org.elasticsearch.utils;

import java.io.IOException;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * @author hanqing.zhq@alibaba-inc.com
 * @date 2018/4/22
 */
public class HttpClientHelper {

    private static OkHttpClient httpClient;

    private HttpClientHelper() { }

    public static synchronized OkHttpClient getHTTPClient() {
        if (null == httpClient) {
            try {
                PermissionHelper.doPrivileged(() -> {
                    httpClient = new OkHttpClient();
                    return httpClient;
                });
            } catch (IOException e) {
                return httpClient;
            }
        }
        return httpClient;
    }

    public static Response httpRequest(String url) throws IOException {
        return PermissionHelper.doPrivileged(() -> {
            Request request = new Request.Builder().get().url(url).build();
            return getHTTPClient().newCall(request).execute();
        });
    }
}
