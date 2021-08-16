package ru.otus.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster

case class HdfsLocalClusterProperties(hdfsNamenodePort: Integer,
                                      hdfsNamenodeHttpPort: Integer = 0,
                                      hdfsTempDir: String,
                                      hdfsNumDatanodes: Integer,
                                      hdfsEnablePermissions: Boolean,
                                      hdfsFormat: Boolean)

class HdfsLocalCluster(properties: HdfsLocalClusterProperties) {
  @transient var localHdfs: MiniDFSCluster = _


  def start(): Unit = {
    val hdfsConf = new Configuration()
    hdfsConf.setBoolean("dfs.permissions", properties.hdfsEnablePermissions)
    System.setProperty("test.build.data", properties.hdfsTempDir)

    localHdfs = new MiniDFSCluster.Builder(hdfsConf)
      .nameNodePort(properties.hdfsNamenodePort)
      .nameNodeHttpPort(properties.hdfsNamenodeHttpPort.intValue)
      .numDataNodes(properties.hdfsNumDatanodes)
      .format(properties.hdfsFormat)
      .racks(null).build


  }

  def stop(): Unit = {
    localHdfs.shutdown
  }

  def getHdfs() = localHdfs.getFileSystem



}


