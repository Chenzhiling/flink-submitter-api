package com.czl.submitter.service

import com.czl.submitter.entity._
import com.czl.submitter.service.tool.FlinkSubmitUtils
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.JobID
import org.apache.flink.client.deployment.application.ApplicationConfiguration
import org.apache.flink.client.program.{ClusterClient, PackagedProgram, PackagedProgramUtils}
import org.apache.flink.configuration._
import org.apache.flink.runtime.jobgraph.{JobGraph, SavepointConfigOptions}
import org.apache.flink.util.FlinkException

import java.io.File
import java.util.Collections
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
import scala.util.Try


/**
 * Author: CHEN ZHI LING
 * Date: 2022/5/26
 * Description:
 */
trait FlinkSubmitTrait {


  def submit(submitRequest: FlinkSubmitRequest): FlinkSubmitResponse = {
    val flinkHome: String = submitRequest.flinkVersion.flinkHome
    val flinkConfig: Configuration = getFlinkDefaultConfiguration(flinkHome)
    flinkConfig
      .safeSet(PipelineOptions.NAME, submitRequest.effectiveAppName)
      .safeSet(DeploymentOptions.TARGET, submitRequest.executionMode.getName)
      .safeSet(SavepointConfigOptions.SAVEPOINT_PATH, submitRequest.savePoint)
      .safeSet(CoreOptions.CLASSLOADER_RESOLVE_ORDER, submitRequest.resolveOrder.getName)
      .safeSet(ApplicationConfiguration.APPLICATION_MAIN_CLASS, submitRequest.mainClass)
      .safeSet(CoreOptions.DEFAULT_PARALLELISM,Integer.valueOf(submitRequest.flinkParallelism))
      .safeSet(ApplicationConfiguration.APPLICATION_ARGS, submitRequest.args)
    setConfig(submitRequest,flinkConfig)
    doSubmit(submitRequest,flinkConfig)
  }


  def stop(stopRequest: FlinkStopRequest): FlinkStopResponse = {
    val flinkConf = new Configuration()
    doStop(stopRequest, flinkConf)
  }


  def query(queryRequest: FlinkQueryRequest): FlinkQueryResponse


  def setConfig(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): Unit


  def doSubmit(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): FlinkSubmitResponse


  def doStop(stopRequest: FlinkStopRequest, flinkConf: Configuration): FlinkStopResponse


  def doQuery(queryRequest: FlinkQueryRequest): FlinkQueryResponse = {
    val master: String = queryRequest.master
    val id: String = queryRequest.jobId
    val url: String = s"$master/jobs/$id"
    FlinkSubmitUtils.queryTaskViaRestApi(url)
  }


  private[submitter] implicit class EnhanceFlinkConfiguration(flinkConfig: Configuration) {
    def safeSet[T](option: ConfigOption[T], value: T): Configuration = {
      flinkConfig match {
        case x if value != null && value.toString.nonEmpty => x.set(option, value)
        case x => x
      }
    }
  }


  /**
   * get flink-conf.yaml
   */
  private[submitter] def getFlinkDefaultConfiguration(flinkHome: String): Configuration = {
    Try(GlobalConfiguration.loadConfiguration(s"$flinkHome/conf")).getOrElse(new Configuration())
  }



  private[submitter] def getOptionFromDefaultFlinkConfig[T](flinkHome: String, option: ConfigOption[T]): T = {
    getFlinkDefaultConfiguration(flinkHome).get(option)
  }


  private[submitter] def getParallelism(flinkSubmitRequest: FlinkSubmitRequest): Integer = {
    if (0 != flinkSubmitRequest.flinkParallelism) {
     flinkSubmitRequest.flinkParallelism
    } else {
      //set parallelism
      getFlinkDefaultConfiguration(flinkSubmitRequest.flinkVersion.flinkHome).getInteger(
        CoreOptions.DEFAULT_PARALLELISM,
        CoreOptions.DEFAULT_PARALLELISM.defaultValue()
      )
    }
  }


  private[submitter] def getJobGraph(flinkConfig: Configuration,
                                     flinkSubmitRequest: FlinkSubmitRequest,
                                     jarFile: File): (PackagedProgram, JobGraph) = {
    val packagedProgram: PackagedProgram = PackagedProgram
      .newBuilder
      //mainClass入口
      .setEntryPointClassName(flinkConfig.getString(ApplicationConfiguration.APPLICATION_MAIN_CLASS))
      .setJarFile(jarFile)
      .setSavepointRestoreSettings(flinkSubmitRequest.savepointRestoreSettings)
      .setArguments(
        flinkConfig
          .getOptional(ApplicationConfiguration.APPLICATION_ARGS)
          .orElse(Collections.emptyList()):_*)
      .build()
    val parallelism: Integer = getParallelism(flinkSubmitRequest)
    val jobGraph: JobGraph = PackagedProgramUtils.createJobGraph(
      packagedProgram,
      flinkConfig,
      parallelism,
      null,
      false
    )
    packagedProgram -> jobGraph
  }


  private[submitter] def cancelJob(stopRequest: FlinkStopRequest, jobID: JobID, client: ClusterClient[_]): String = {
    //savepoint path
    val savePointDir: String = {
      if (!stopRequest.withSavePoint) null
      else {
        if (null != stopRequest.customSavePointPath) {
          stopRequest.customSavePointPath
        } else {
          val configDir: String = getOptionFromDefaultFlinkConfig[String](
            stopRequest.flinkVersion.flinkHome,
            ConfigOptions.key(CheckpointingOptions.SAVEPOINT_DIRECTORY.key())
              .stringType()
              .defaultValue(null)
          )
          if (StringUtils.isEmpty(configDir)) {
            throw new FlinkException(s"executionMode: ${stopRequest.executionMode.getName}, savePoint path is null or invalid.")
          } else {
            configDir
          }
        }
      }
    }
    (Try(stopRequest.withSavePoint).getOrElse(false), Try(stopRequest.withDrain).getOrElse(false)) match {
      case (false, false) =>
        client.cancel(jobID).get()
        null
      case (true, false) => client.cancelWithSavepoint(jobID, savePointDir).get(10000, TimeUnit.MILLISECONDS)
      case (_, _) => client.stopWithSavepoint(jobID, stopRequest.withDrain, savePointDir).get(10000, TimeUnit.MILLISECONDS)
    }
  }
}

