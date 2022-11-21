package com.czl.submitter.service.impl

import com.czl.submitter.entity.{FlinkQueryRequest, FlinkQueryResponse, FlinkStopRequest, FlinkStopResponse, FlinkSubmitRequest, FlinkSubmitResponse}
import com.czl.submitter.service.FlinkSubmitTrait
import com.czl.submitter.service.tool.FlinkSubmitUtils
import org.apache.flink.api.common.JobID
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader, StandaloneClusterDescriptor, StandaloneClusterId}
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration.{Configuration, DeploymentOptions, RestOptions}

import java.io.File
import java.lang.{Integer => JavaInt}

/**
 * Author: CHEN ZHI LING
 * Date: 2022/5/27
 * Description:
 */
object RemoteSubmit extends FlinkSubmitTrait {


  override def doSubmit(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): FlinkSubmitResponse = {
    restfulApiSubmit(flinkConf,submitRequest.supportTaskJarFile)
  }


  override def setConfig(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): Unit = {
    flinkConf
      .safeSet(RestOptions.ADDRESS, submitRequest.extraParameter.get(RestOptions.ADDRESS.key()).toString)
      .safeSet[JavaInt](RestOptions.PORT, submitRequest.extraParameter.get(RestOptions.PORT.key()).toString.toInt)
  }


  override def doStop(stopRequest: FlinkStopRequest, flinkConf: Configuration): FlinkStopResponse = {
    flinkConf
      .safeSet(DeploymentOptions.TARGET, stopRequest.executionMode.getName)
      .safeSet(RestOptions.ADDRESS, stopRequest.extraParameter.get(RestOptions.ADDRESS.key()).toString)
      .safeSet[JavaInt](RestOptions.PORT, stopRequest.extraParameter.get(RestOptions.PORT.key()).toString.toInt)
    val standAloneDescriptor: (StandaloneClusterId, StandaloneClusterDescriptor) =
      getStandAloneClusterDescriptor(flinkConf)
    var client: ClusterClient[StandaloneClusterId] = null
    try {
      client = standAloneDescriptor._2.retrieve(standAloneDescriptor._1).getClusterClient
      val jobID: JobID = JobID.fromHexString(stopRequest.jobId)
      val actionResult: String = cancelJob(stopRequest, jobID, client)
      FlinkStopResponse(actionResult)
    } catch {
      case e: Exception =>
        throw e
    } finally {
      if (client != null) client.close()
      if (standAloneDescriptor != null) standAloneDescriptor._2.close()
    }
  }


  override def query(queryRequest: FlinkQueryRequest): FlinkQueryResponse = {
    this.doQuery(queryRequest)
  }


  def restfulApiSubmit(flinkConfig: Configuration, fatJar: File): FlinkSubmitResponse = {
    var clusterDescriptor: StandaloneClusterDescriptor = null
    var client: ClusterClient[StandaloneClusterId] = null
    try {
      val standAloneDescriptor: (StandaloneClusterId, StandaloneClusterDescriptor) = getStandAloneClusterDescriptor(flinkConfig)
      val yarnClusterId: StandaloneClusterId = standAloneDescriptor._1
      clusterDescriptor = standAloneDescriptor._2

      client = clusterDescriptor.retrieve(yarnClusterId).getClusterClient
      val jobId: String = FlinkSubmitUtils.submitJarViaRestApi(client.getWebInterfaceURL, fatJar, flinkConfig)
      FlinkSubmitResponse(client.getClusterId.toString, jobId)
    } catch {
      case e: Exception =>
        throw e
    }
  }


  def getStandAloneClusterDescriptor(flinkConfig: Configuration): (StandaloneClusterId, StandaloneClusterDescriptor) = {
    val serviceLoader = new DefaultClusterClientServiceLoader
    val clientFactory: ClusterClientFactory[Nothing] = serviceLoader.getClusterClientFactory(flinkConfig)
    val standaloneClusterId: StandaloneClusterId = clientFactory.getClusterId(flinkConfig)
    //connect to flink standalone
    val standaloneClusterDescriptor: StandaloneClusterDescriptor =
      clientFactory.createClusterDescriptor(flinkConfig).asInstanceOf[StandaloneClusterDescriptor]
    (standaloneClusterId, standaloneClusterDescriptor)
  }
}
