package org.elasticsearch.aliyun.oss.blobstore;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Map;

/**
 * A class for managing a oss repository of blob entries, where each blob entry is just a named group of bytes
 * Created by yangkongshi on 2017/11/24.
 */
public class OssBlobContainer extends AbstractBlobContainer {
    protected final Logger logger = Loggers.getLogger(OssBlobContainer.class);
    protected final OssBlobStore blobStore;
    protected final String keyPath;

    public OssBlobContainer(BlobPath path, OssBlobStore blobStore) {
        super(path);
        this.keyPath = path.buildAsString();
        this.blobStore = blobStore;
    }

    /**
     * Tests whether a blob with the given blob name exists in the container.
     *
     * @param blobName The name of the blob whose existence is to be determined.
     * @return {@code true} if a blob exists in the BlobContainer with the given name, and {@code false} otherwise.
     */
    @Override public boolean blobExists(String blobName) {
        logger.trace("blobExists({})", blobName);
        try {
            return blobStore.blobExists(buildKey(blobName));
        } catch (OSSException | ClientException | IOException e) {
            logger.warn("can not access [{}] in bucket {{}}: {}", blobName, blobStore.getBucket(),
                e.getMessage());
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    /**
     * Creates a new {@link InputStream} for the given blob name.
     *
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @return The {@code InputStream} to read the blob.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException         if the blob can not be read.
     */
    @Override public InputStream readBlob(String blobName) throws IOException {
        logger.trace("readBlob({})", blobName);
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("[" + blobName + "] blob not found");
        }
        return blobStore.readBlob(buildKey(blobName));
    }


    /**
     * Reads blob content from the input stream and writes it to the container in a new blob with the given name.
     * This method assumes the container does not already contain a blob of the same blobName.  If a blob by the
     * same name already exists, the operation will fail and an {@link IOException} will be thrown.
     *
     * @param blobName    The name of the blob to write the contents of the input stream to.
     * @param inputStream The input stream from which to retrieve the bytes to write to the blob.
     * @param blobSize    The size of the blob to be written, in bytes.  It is implementation dependent whether
     *                    this value is used in writing the blob to the repository.
     * @throws FileAlreadyExistsException if a blob by the same name already exists
     * @throws IOException                if the input stream could not be read, or the target blob could not be written to.
     */
    @Override public void writeBlob(String blobName, InputStream inputStream, long blobSize)
        throws IOException {
        if (blobExists(blobName)) {
            throw new FileAlreadyExistsException(
                "blob [" + blobName + "] already exists, cannot overwrite");
        }
        logger.trace("writeBlob({}, stream, {})", blobName, blobSize);
        blobStore.writeBlob(buildKey(blobName), inputStream, blobSize);
    }



    /**
     * Deletes a blob with giving name, if the blob exists.  If the blob does not exist, this method throws an IOException.
     *
     * @param blobName The name of the blob to delete.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException         if the blob exists but could not be deleted.
     */
    @Override public void deleteBlob(String blobName) throws IOException {
        logger.trace("deleteBlob({})", blobName);
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }
        try {
            blobStore.deleteBlob(buildKey(blobName));
        } catch (OSSException | ClientException e) {
            logger.warn("can not access [{}] in bucket {{}}: {}", blobName, blobStore.getBucket(),
                e.getMessage());
            throw new IOException(e);
        }

    }

    /**
     * Lists all blobs in the container.
     *
     * @return A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     * the values are {@link BlobMetaData}, containing basic information about each blob.
     * @throws IOException if there were any failures in reading from the blob container.
     */
    @Override public Map<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    /**
     * Lists all blobs in the container.
     *
     * @return A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     * the values are {@link BlobMetaData}, containing basic information about each blob.
     * @throws IOException if there were any failures in reading from the blob container.
     */
    @Override public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix)
        throws IOException {
        logger.trace("listBlobsByPrefix({})", blobNamePrefix);
        try {
            return blobStore.listBlobsByPrefix(keyPath, blobNamePrefix);
        } catch (IOException e) {
            logger.warn("can not access [{}] in bucket {{}}: {}", blobNamePrefix,
                blobStore.getBucket(), e.getMessage());
            throw new IOException(e);
        }
    }

    /**
     * Renames the source blob into the target blob.  If the source blob does not exist or the
     * target blob already exists, an exception is thrown.  Atomicity of the move operation
     * can only be guaranteed on an implementation-by-implementation basis.  The only current
     * implementation of {@link BlobContainer} for which atomicity can be guaranteed is the
     * {@link org.elasticsearch.common.blobstore.fs.FsBlobContainer}.
     *
     * @param sourceBlobName The blob to rename.
     * @param targetBlobName The name of the blob after the renaming.
     * @throws IOException if the source blob does not exist, the target blob already exists,
     *                     or there were any failures in reading from the blob container.
     */
    @Override public void move(String sourceBlobName, String targetBlobName) throws IOException {
        logger.trace("move({}, {})", sourceBlobName, targetBlobName);
        if (!blobExists(sourceBlobName)) {
            throw new IOException("Blob [" + sourceBlobName + "] does not exist");
        } else if (blobExists(targetBlobName)) {
            throw new IOException("Blob [" + targetBlobName + "] has already exist");
        }
        try {
            blobStore.move(buildKey(sourceBlobName), buildKey(targetBlobName));
        } catch (OSSException | ClientException | NoSuchFileException e) {
            logger.warn("can not move blob [{}] to [{}] in bucket {{}}: {}", sourceBlobName,
                targetBlobName, blobStore.getBucket(), e.getMessage());
            throw new IOException(e);
        }
    }

    protected String buildKey(String blobName) {
        return keyPath + (blobName == null ? StringUtils.EMPTY : blobName);
    }
}
