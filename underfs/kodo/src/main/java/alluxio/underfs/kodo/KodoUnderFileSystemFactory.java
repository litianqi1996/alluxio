package alluxio.underfs.kodo;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;

public class KodoUnderFileSystemFactory implements UnderFileSystemFactory {


  public KodoUnderFileSystemFactory() {
  }

  @Override
  public UnderFileSystem create(String path, @Nullable UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");
    return KodoUnderFileSystem.creatInstance(new AlluxioURI(path), conf);
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_KODO);
  }
}
