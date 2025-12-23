package com.springml.spark.sftp

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * Simple unit test for basic testing on different formats of file
 */
class TestDatasetRelation extends AnyFunSuite with BeforeAndAfterEach {
  var ss: SparkSession = _

  override def beforeEach() {
    ss = SparkSession.builder().master("local").enableHiveSupport().appName("Test Dataset Relation").getOrCreate()
  }

  test ("Read CSV") {
    val fileLocation = getClass.getResource("/sample.csv").getPath
    val dsr = DatasetRelation(fileLocation, "csv", "false", "true", ",", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(3 == rdd.count())
  }

  test ("Read CSV using custom delimiter") {
    val fileLocation = getClass.getResource("/sample.csv").getPath
    val dsr = DatasetRelation(fileLocation, "csv", "false", "true", ";", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(3 == rdd.count())
  }

  test ("Read multiline CSV using custom quote and escape") {
    val fileLocation = getClass.getResource("/sample_quoted_multiline.csv").getPath
    val dsr = DatasetRelation(fileLocation, "csv", "false", "true", ",", "\"", "\\", "true", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(3 == rdd.count())
  }


  test ("Read JSON") {
    val fileLocation = getClass.getResource("/people.json").getPath
    val dsr = DatasetRelation(fileLocation, "json", "false", "true", ",", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(3 == rdd.count())
  }

  test ("Read AVRO") {
    val fileLocation = getClass.getResource("/users.avro").getPath
    val dsr = DatasetRelation(fileLocation, "avro", "false", "true", ",", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(2 == rdd.count())
  }

  test ("Read parquet") {
    val fileLocation = getClass.getResource("/users.parquet").getPath
    val dsr = DatasetRelation(fileLocation, "parquet", "false", "true", ",", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(2 == rdd.count())
  }

  test ("Write and Read Parquet") {
    val spark = ss
    import spark.implicits._
    // create a small dataframe and write it out as parquet
    val df = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val tempDir = java.nio.file.Files.createTempDirectory("parquet_roundtrip").toFile.getAbsolutePath
    val outPath = tempDir + "/out"
    df.coalesce(1).write.parquet(outPath)

    // read it back using DatasetRelation and assert content
    val dsr = DatasetRelation(outPath, "parquet", "false", "true", ",", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(2 == rdd.count())
    val names = rdd.map(_.getString(1)).collect().toSet
    assert(names == Set("Alice", "Bob"))
  }

  test ("SFTP write and read (localhost:2222)") {
    // Skip this test if the SFTP server is not available
    try {
      val sock = new java.net.Socket()
      val addr = new java.net.InetSocketAddress("localhost", 2222)
      sock.connect(addr, 2000)
      sock.close()
    } catch {
      case _: Throwable => cancel("SFTP server not available at localhost:2222 - skipping")
    }

    val spark = ss
    import spark.implicits._
    val df = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")

    // ensure /upload directory exists on the SFTP server
    try {
      val jsch = new com.jcraft.jsch.JSch()
      val session = jsch.getSession("foo", "localhost", 2222)
      session.setPassword("pass")
      val config = new java.util.Properties()
      config.put("StrictHostKeyChecking", "no")
      session.setConfig(config)
      session.connect(3000)
      val channel = session.openChannel("sftp").asInstanceOf[com.jcraft.jsch.ChannelSftp]
      channel.connect()
      try {
        try { channel.ls("/upload") } catch { case _: Throwable => channel.mkdir("/upload") }
      } finally {
        channel.disconnect(); session.disconnect()
      }
    } catch {
      case _: Throwable => // ignore directory-create failures; upload may still work if server auto-creates
    }

    // write to SFTP (use path under /upload)
    val writeParams = Map(
      "host" -> "localhost",
      "port" -> "2222",
      "username" -> "foo",
      "password" -> "pass",
      "path" -> "/upload/test_sftp.csv",
      "fileType" -> "csv",
      "header" -> "true"
    )
    new DefaultSource().createRelation(ss.sqlContext, org.apache.spark.sql.SaveMode.Overwrite, writeParams, df)

    // read back from SFTP
    val tempDir = java.nio.file.Files.createTempDirectory("sftp_roundtrip").toFile.getAbsolutePath
    val readParams = Map(
      "host" -> "localhost",
      "port" -> "2222",
      "username" -> "foo",
      "password" -> "pass",
      "path" -> "/upload/test_sftp.csv",
      "fileType" -> "csv",
      "header" -> "true",
      "tempLocation" -> tempDir
    )
    val relation = new DefaultSource().createRelation(ss.sqlContext, readParams)
    val dsr = relation.asInstanceOf[DatasetRelation]
    val rdd = dsr.buildScan()
    assert(2 == rdd.count())
    val names = rdd.map(_.getString(1)).collect().toSet
    assert(names == Set("Alice", "Bob"))
  }

  test ("SFTP read parquet (localhost:2222)") {
    // Skip if server is unreachable
    try {
      val sock = new java.net.Socket()
      val addr = new java.net.InetSocketAddress("localhost", 2222)
      sock.connect(addr, 2000)
      sock.close()
    } catch {
      case _: Throwable => cancel("SFTP server not available at localhost:2222 - skipping")
    }

    val spark = ss
    import spark.implicits._
    // create a small parquet file locally
    val df = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val tempDir = java.nio.file.Files.createTempDirectory("sftp_parquet").toFile.getAbsolutePath
    val localParquetDir = tempDir + "/sample.parquet"
    df.coalesce(1).write.parquet(localParquetDir)

    // find the part parquet file produced
    val partFile = new java.io.File(localParquetDir).listFiles().filter(_.getName.endsWith(".parquet")).head.getAbsolutePath

    // ensure /upload exists and upload the parquet file to /upload/sample.parquet
    try {
      val jsch = new com.jcraft.jsch.JSch()
      val session = jsch.getSession("foo", "localhost", 2222)
      session.setPassword("pass")
      val config = new java.util.Properties()
      config.put("StrictHostKeyChecking", "no")
      session.setConfig(config)
      session.connect(3000)
      val channel = session.openChannel("sftp").asInstanceOf[com.jcraft.jsch.ChannelSftp]
      channel.connect()
      try {
        try { channel.ls("/upload") } catch { case _: Throwable => channel.mkdir("/upload") }
        channel.put(partFile, "/upload/sample.parquet")
      } finally {
        channel.disconnect(); session.disconnect()
      }
    } catch {
      case e: Throwable => cancel("Failed to upload parquet to SFTP server: " + e.getMessage)
    }

    // read back via DatasetRelation
    val readTempDir = java.nio.file.Files.createTempDirectory("sftp_parquet_read").toFile.getAbsolutePath
    val readParams = Map(
      "host" -> "localhost",
      "port" -> "2222",
      "username" -> "foo",
      "password" -> "pass",
      "path" -> "/upload/sample.parquet",
      "fileType" -> "parquet",
      "tempLocation" -> readTempDir
    )
    val relation = new DefaultSource().createRelation(ss.sqlContext, readParams)
    val dsr = relation.asInstanceOf[DatasetRelation]
    val rdd = dsr.buildScan()
    assert(2 == rdd.count())
    val names = rdd.map(_.getString(1)).collect().toSet
    assert(names == Set("Alice", "Bob"))
  }

  test ("Read text file") {
    val fileLocation = getClass.getResource("/plaintext.txt").getPath
    val dsr = DatasetRelation(fileLocation, "txt", "false", "true", ",", "\"", "\\", "false", null, null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(3 == rdd.count())
  }

  test ("Read xml file") {
    val fileLocation = getClass.getResource("/books.xml").getPath
    val dsr = DatasetRelation(fileLocation, "xml", "false", "true", ",", "\"", "\\", "false", "book", null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(12 == rdd.count())
  }
  test ("Read orc file") {
    val fileLocation = getClass.getResource("/books.orc").getPath
    val dsr = DatasetRelation(fileLocation, "orc", "false", "true", ",", "\"", "\\", "false", "book", null, ss.sqlContext)
    val rdd = dsr.buildScan()
    assert(12 == rdd.count())
  }
}
