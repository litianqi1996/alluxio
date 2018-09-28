package alluxio.underfs.kodo;

import alluxio.underfs.MultiRangeObjectInputStream;

import java.io.IOException;
import java.io.InputStream;


public class KodoInputStream extends MultiRangeObjectInputStream {


  /**
   * Key of the file in Kodo to read.
   */
  private final String mKey;

  /**
   * The Kodo client for Kodo operations.
   */
  private final KodoClient mKodoclent;

  /**
   * The size of the object in bytes.
   */
  private final long mContentLength;


  KodoInputStream(String bucketname, String key, KodoClient kodoClient) throws Exception {
    this(key, kodoClient, 0L);
  }

  KodoInputStream(String key, KodoClient kodoClient, long position)
      throws Exception {
    mKey = key;
    mKodoclent = kodoClient;
    mPos = position;
    mContentLength = kodoClient.getFileInfo(key).fsize;
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
    return mKodoclent.getObject(mKey, startPos, endPos);
  }
}

