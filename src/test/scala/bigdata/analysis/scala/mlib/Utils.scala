package bigdata.analysis.scala.mlib

import java.io.{File, IOException}
import java.util.UUID

/**
  * Created by frank on 17-3-15.
  */
object Utils {
  /**
    * Create a directory inside the given parent directory. The directory is guaranteed to be
    * newly created, and is not marked for automatic deletion.
    */
  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = 2
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }

    dir.getCanonicalFile
  }

  /**
    * Create a temporary directory inside the given parent directory. The directory will be
    * automatically deleted when the VM shuts down.
    */
  def createTempDir(
                     root: String = System.getProperty("java.io.tmpdir"),
                     namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    //ShutdownHookManager.registerShutdownDeleteDir(dir)
    dir
  }
}
