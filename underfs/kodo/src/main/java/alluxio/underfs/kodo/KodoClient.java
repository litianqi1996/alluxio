package alluxio.underfs.kodo;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;
import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KodoClient {

  private static final Logger LOG = LoggerFactory.getLogger(KodoOutputStream.class);
  private BucketManager bucketManager;
  private UploadManager uploadManager;
  private String cdnHost;
  private Auth mauth;


  public KodoClient(Auth auth, String Host, Configuration cfg) {
    mauth = auth;
    cdnHost = Host;
    bucketManager = new BucketManager(mauth, cfg);
    uploadManager = new UploadManager(cfg);
  }


  public FileInfo getFileInfo(String bucketname, String key) throws Exception {
    return bucketManager.stat(bucketname, key);
  }


  public InputStream getObject(String mBucketName, String mKey, long startPos, long endPos) {

    String ObjectUrl = mauth.privateDownloadUrl("http://" + cdnHost + "/" + mKey);
    LOG.info("Qiniu:ObjectUrl" + ObjectUrl + "\n");
    HttpURLConnection con = null;
    try {
      URL url = new URL(ObjectUrl);
      con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");
      String rangeRequest = "bytes=" + String.valueOf(startPos) + "-" + String.valueOf(endPos - 1);
      con.setRequestProperty("Range", rangeRequest);
      return con.getInputStream();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public void uploadFile(String mBucketName, String mKey, File mFile) throws Exception {
    uploadManager.put(mFile, mKey, mauth.uploadToken(mBucketName, mKey));
  }

  public boolean copyObject(String mBucketName, String src, String dst) {
    try {
      bucketManager.copy(mBucketName, src, mBucketName, dst);
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  public boolean createEmptyObject(String bucketname, String key) {
    try {
      uploadManager.put(new byte[0], key, mauth.uploadToken(bucketname, key));
      return true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  public boolean deleteObject(String mBucketName, String key) {
    try {
      bucketManager.delete(mBucketName, key);
      return true;
    } catch (Exception e) {
      e.printStackTrace();

      return false;
    }
  }

  public FileListing listFiles(String bucketname, String prefix, String marker, int limit,
      String delimiter) {
    try {
      return bucketManager.listFiles(bucketname, prefix, marker, limit, delimiter);
    } catch (QiniuException e) {
      e.printStackTrace();
    }
    return null;
  }


}
