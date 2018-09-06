package alluxio.underfs.kodo;


import alluxio.underfs.MultiRangeObjectInputStream;
import java.io.IOException;
import java.io.InputStream;


public class KodoInputStream extends MultiRangeObjectInputStream {

  /**
   * Bucket name of the Alluxio  bucket.
   */
  private final String mBucketName;

  /**
   * Key of the file in OSS to read.
   */
  private final String mKey;

  /**
   * The OSS client for OSS operations.
   */
  private final KodoClient mKodoclent;

  /**
   * The size of the object in bytes.
   */
  private final long mContentLength;


  KodoInputStream(String bucketname, String key, KodoClient kodoClient) throws Exception {
    this(bucketname, key, kodoClient, 0L);
  }

  KodoInputStream(String bucketName, String key, KodoClient kodoClient, long position)
      throws Exception {
    mBucketName = bucketName;
    mKey = key;
    mKodoclent = kodoClient;
    mPos = position;
    mContentLength = kodoClient.getFileInfo(bucketName, key).fsize;
  }

  /**
   * Open a new stream reading a range. When endPos > content length, the returned stream should
   * read till the last valid byte of the input. The behaviour is undefined when (startPos < 0),
   * (startPos >= content length), or (endPos <= 0).
   *
   * @param startPos start position in bytes (inclusive)
   * @param endPos end position in bytes (exclusive)
   * @return a new {@link InputStream}
   */
  @Override
  protected InputStream createStream(long startPos, long endPos) throws IOException {
    return mKodoclent.getObject(mBucketName, mKey, startPos, endPos);
  }
}

