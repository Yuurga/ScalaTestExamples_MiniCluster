package ru.otus.spark

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class LocalHdfsTest extends FunSuite with BeforeAndAfterAll {

  @transient private var localHdfs: HdfsLocalCluster = _

  override def beforeAll() {
    val prop = HdfsLocalClusterProperties(hdfsNamenodePort = 20112,
      hdfsNamenodeHttpPort = 50070,
      hdfsTempDir = "embedded_hdfs",
      hdfsNumDatanodes = 1,
      hdfsEnablePermissions = false,
      hdfsFormat = true)

    localHdfs = new HdfsLocalCluster(prop)
    localHdfs.start()
    super.beforeAll()
  }

  override def afterAll() {
    localHdfs.stop()
    super.afterAll()
  }

  test("Write to localHdfs") {

    val hdfsFs = localHdfs.getHdfs()
    val writer: FSDataOutputStream = hdfsFs.create(new Path("/tmp/testing"))
    writer.writeUTF("some text")
    writer.close()

    // Read the file and compare to test string
    val reader: FSDataInputStream = hdfsFs.open(new Path("/tmp/testing"))
    val res = reader.readUTF()
    println(res)
    assertResult("some text") {
      res
    }
    reader.close()


  }

}
