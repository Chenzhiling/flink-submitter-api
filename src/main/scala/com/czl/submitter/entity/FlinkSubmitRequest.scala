package com.czl.submitter.entity

import com.czl.submitter.FlinkInfo
import com.czl.submitter.enums.{ExecutionMode, ResolveOrder}
import org.apache.flink.runtime.jobgraph.{SavepointConfigOptions, SavepointRestoreSettings}

import java.io.File
import java.util.{List => JavaList, Map => JavaMap}
import scala.util.Try

/**
 * Author: CHEN ZHI LING
 * Date: 2022/5/26
 * Description:
 */
case class FlinkSubmitRequest(flinkVersion: FlinkInfo,
                              executionMode: ExecutionMode,
                              resolveOrder: ResolveOrder,
                              appName: String,
                              mainClass: String,
                              flinkJarPath: String,
                              savePoint: String,
                              flinkParallelism: Int,
                              args: JavaList[String],
                              extraParameter: JavaMap[String, Any]) {


  lazy val effectiveAppName: String = if (this.appName == null) "flink-task" else this.appName


  lazy val supportTaskJarFile: File = {
        new File(flinkJarPath)
  }


  lazy val savepointRestoreSettings: SavepointRestoreSettings = {
    lazy val allowNonRestoredState: Boolean = Try(
      extraParameter.get(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key).toString.toBoolean).getOrElse(false)
    savePoint match {
      case sp if Try(sp.isEmpty).getOrElse(true) => SavepointRestoreSettings.none
      case sp => SavepointRestoreSettings.forPath(sp, allowNonRestoredState)
    }
  }
}
