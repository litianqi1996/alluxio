package alluxio.underfs.kodo;

import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;


@NotThreadSafe
public class KodoOutputStream extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(KodoOutputStream.class);


  private final String mKey;

  private final File mFile;

  private final KodoClient mKodoClient;

  private OutputStream mLocalOutputStream;

  private MessageDigest mHash;

  /**
   * Flag to indicate this stream has been closed, to ensure close is only done once.
   */
  private AtomicBoolean mClosed = new AtomicBoolean(false);

  public KodoOutputStream(String key, KodoClient kodoClient) throws IOException {
    mKey = key;
    mKodoClient = kodoClient;
    mFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(), UUID.randomUUID()));

    try {
      mHash = MessageDigest.getInstance("MD5");
      mLocalOutputStream =
          new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(mFile), mHash));
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Algorithm not available for MD5 hash.", e);
      mHash = null;
      mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    }
  }

  @Override
  public void write(int b) throws IOException {
    mLocalOutputStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mLocalOutputStream.write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mLocalOutputStream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    mLocalOutputStream.flush();
  }

  /**
   * Closes this output stream. When an output stream is closed, the local temporary file is
   * uploaded to KODO Service. Once the file is uploaded, the temporary file is deleted.
   */

  @Override
  public void close() {
    if (mClosed.getAndSet(true)) {
      return;
    }
    try {
      mLocalOutputStream.close();
      mKodoClient.uploadFile(mKey, mFile);
    } catch (Exception e) {
      e.printStackTrace();
    }
    mFile.delete();
  }

}
