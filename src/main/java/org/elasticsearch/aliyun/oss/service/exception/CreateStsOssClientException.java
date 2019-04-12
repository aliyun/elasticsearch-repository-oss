package org.elasticsearch.aliyun.oss.service.exception;

import java.io.IOException;

/**
 * @author hanqing.zhq@alibaba-inc.com
 * @date 2018/4/17
 */
public class CreateStsOssClientException extends IOException {
    public CreateStsOssClientException(Throwable e) {
        super(e);
    }

    public CreateStsOssClientException(String s) {
        super(s);
    }
}
