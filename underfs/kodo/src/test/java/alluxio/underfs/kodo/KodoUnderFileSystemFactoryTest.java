package alluxio.underfs.kodo;

import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link KodoUnderFileSystemFactoryTest}.
 */
public class KodoUnderFileSystemFactoryTest {

  /**
   * Tests that the KODO UFS module correctly accepts paths that begin with kodo://.
   */
  @Test
  public void factory() {
    UnderFileSystemFactory factory = UnderFileSystemFactoryRegistry.find("kodo://test-bucket/path");

    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for oss paths when using this module", factory);
  }
}
