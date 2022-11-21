package com.czl.submitter

import java.io.File
import java.net.{URL => NetURL}
import java.util.regex.{Matcher, Pattern}

/**
 * Author: CHEN ZHI LING
 * <br>
 * Date: 2022/5/12 16:10
 * <br>
 * Description: get flink info by flink home
 */
class FlinkInfo(val flinkHome: String) extends Serializable {



  private[this] lazy val FLINK_SCALA_VERSION_PATTERN: Pattern = Pattern.compile("^flink-dist_(.*)-(.*).jar$")


  lazy val fullVersion: String = s"${version}_$scalaVersion"


  /**
   * get scala version
   */
  lazy val scalaVersion: String = {
    val matcher: Matcher = FLINK_SCALA_VERSION_PATTERN.matcher(flinkDistJar.getName)
    matcher.matches()
    matcher.group(1)
  }


  /**
   * get Flink version
   */
  lazy val version: String = {
    val matcher: Matcher = FLINK_SCALA_VERSION_PATTERN.matcher(flinkDistJar.getName)
    matcher.matches()
    matcher.group(2)
  }


  /**
   * get all flink libs
   */
  lazy val flinkLib: File = {
    require(flinkHome != null, "flinkHome can not be null")
    require(new File(flinkHome).exists(), "flinkHome not exist")
    val lib = new File(s"$flinkHome/lib")
    require(lib.exists() && lib.isDirectory, s"$flinkHome/lib should be directory")
    lib
  }


  /**
   * yarn-ship content
   */
  lazy val flinkYarnShipFiles: List[String] = {
    List(flinkLib.toString,s"$flinkHome/plugins",s"$flinkHome/conf/log4j.properties")
  }


  /**
   * get flink distJar
   */
  lazy val flinkDistJar: File = {
    val distJar: Array[File] = flinkLib.listFiles().filter((_: File).getName.matches("flink-dist_.*\\.jar"))
    distJar match {
      case x if x.isEmpty =>
        throw new IllegalArgumentException(s"no flink-dist.jar in $flinkLib")
      case x if x.length > 1 =>
        throw new IllegalArgumentException(s"there are multiple flink-dist.jar in $flinkLib ")
      case _ =>
    }
    distJar.head
  }
}
