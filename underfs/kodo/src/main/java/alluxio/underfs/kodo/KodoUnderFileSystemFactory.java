package alluxio.underfs.kodo;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Factory for creating {@link KodoUnderFileSystem}.
 */
public class KodoUnderFileSystemFactory implements UnderFileSystemFactory {
  /**
   * Constructs a new {@link KodoUnderFileSystem}.
   */
  public KodoUnderFileSystemFactory() {}

  /**
   *
   * @param path file path
   * @param conf optional configuration object for the UFS, may be null
   * @return
   */
  @Override
  public UnderFileSystem create(String path, @Nullable UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");
    return KodoUnderFileSystem.creatInstance(new AlluxioURI(path), conf);
  }

  /**
   *
   * @param path file path
   * @return
   */
  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_KODO);
  }
}
