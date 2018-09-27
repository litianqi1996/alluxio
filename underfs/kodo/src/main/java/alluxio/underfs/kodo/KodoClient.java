package alluxio.underfs.kodo;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class KodoClient {

  private static final Logger LOG = LoggerFactory.getLogger(KodoOutputStream.class);
  private BucketManager mBucketManager;
  private UploadManager mUploadManager;
  private String mSourceHost;
  private String mEndPoint;
  private Auth mAuth;
  private OkHttpClient mOkHttpClient;


  public KodoClient(Auth auth, String SourceHost,String EndPoint,Configuration cfg,OkHttpClient.Builder builder) {
    mAuth = auth;
    mEndPoint=EndPoint;
    mSourceHost = SourceHost;
    mBucketManager = new BucketManager(mAuth, cfg);
    mUploadManager = new UploadManager(cfg);
    mOkHttpClient  = builder.build();
  }


  public FileInfo getFileInfo(String bucketname, String key) throws Exception {
    return mBucketManager.stat(bucketname, key);
  }


  public InputStream getObject(String key,long startPos,long endPos ) throws IOException{
    //All requests are authenticated by default
    // default expires 3600s
    String baseUrl= String.format("http://%s/%s",mSourceHost,key);
    String privateUrl = mAuth.privateDownloadUrl(baseUrl);
    URL url = new URL(privateUrl);
    String ObjectUrl =String.format("http://%s/%s?%s",mEndPoint,key,url.getQuery());
    try {
       Request request = new Request.Builder().url(ObjectUrl)
           .addHeader("Range","bytes=" + String.valueOf(startPos) + "-" + String.valueOf(endPos - 1))
           .addHeader("Host",mSourceHost)
           .get()
           .build();
       Response response = mOkHttpClient.newCall(request).execute();
       return response.body().byteStream();
     }catch (Exception e){
       e.printStackTrace();
     }
     return null;
  }

  public void uploadFile(String mBucketName, String mKey, File mFile) throws Exception {
    mUploadManager.put(mFile, mKey, mAuth.uploadToken(mBucketName, mKey));
  }

  public boolean copyObject(String mBucketName, String src, String dst) {
    try {
      mBucketManager.copy(mBucketName, src, mBucketName, dst);
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  public boolean createEmptyObject(String bucketname, String key) {
    try {
      mUploadManager.put(new byte[0], key, mAuth.uploadToken(bucketname, key));
      return true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  public boolean deleteObject(String mBucketName, String key) {
    try {
      mBucketManager.delete(mBucketName, key);
      return true;
    } catch (Exception e) {
      e.printStackTrace();

      return false;
    }
  }

  public FileListing listFiles(String bucketname, String prefix, String marker, int limit,
      String delimiter) {
    try {
      return mBucketManager.listFiles(bucketname, prefix, marker, limit, delimiter);
    } catch (QiniuException e) {

      e.printStackTrace();
    }
    return null;
  }


}
