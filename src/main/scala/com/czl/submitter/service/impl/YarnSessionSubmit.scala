package com.czl.submitter.service.impl

import com.czl.submitter.FlinkInfo
import com.czl.submitter.consts.FlinkConst.KEY_YARN_APP_ID
import com.czl.submitter.entity._
import com.czl.submitter.service.FlinkSubmitTrait
import org.apache.flink.api.common.JobID
import org.apache.flink.client.deployment.{ClusterClientFactory, ClusterSpecification, DefaultClusterClientServiceLoader}
import org.apache.flink.client.program.{ClusterClient, ClusterClientProvider, PackagedProgram}
import org.apache.flink.configuration.{Configuration, DeploymentOptions}
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.yarn.YarnClusterDescriptor
import org.apache.flink.yarn.configuration.{YarnConfigOptions, YarnDeploymentTarget}
import org.apache.hadoop.yarn.api.records.{ApplicationId, FinalApplicationStatus}

import scala.collection.JavaConverters._

/**
 * Author: CHEN ZHI LING
 * Date: 2022/6/10
 * Description:
 */
object YarnSessionSubmit extends FlinkSubmitTrait {


  override def setConfig(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): Unit = {
    flinkConf
      .safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName)
      .safeSet(YarnConfigOptions.APPLICATION_ID, submitRequest.extraParameter.get(KEY_YARN_APP_ID).toString)
  }


  override def doSubmit(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): FlinkSubmitResponse = {
    var clusterDescriptor: YarnClusterDescriptor = null
    var packageProgram: PackagedProgram = null
    var client: ClusterClient[ApplicationId] = null
    try {
      val yarnClusterDescriptor: (ApplicationId, YarnClusterDescriptor) = getYarnSessionClusterDescriptor(flinkConf)
      val yarnClusterId: ApplicationId = yarnClusterDescriptor._1
      clusterDescriptor = yarnClusterDescriptor._2
      val packageProgramJobGraph: (PackagedProgram, JobGraph) = {
        super.getJobGraph(flinkConf, submitRequest, submitRequest.supportTaskJarFile)
      }
      packageProgram = packageProgramJobGraph._1
      val jobGraph: JobGraph = packageProgramJobGraph._2


      client = clusterDescriptor.retrieve(yarnClusterId).getClusterClient
      val jobId: String = client.submitJob(jobGraph).get().toString
      FlinkSubmitResponse(yarnClusterId.toString, jobId)
    } catch {
      case exception: Exception =>
        throw exception
    } finally {
      if(null != packageProgram) packageProgram.close()
      if(null != client) client.close()
      if(null != clusterDescriptor) clusterDescriptor.close()
    }
  }


  override def doStop(stopRequest: FlinkStopRequest, flinkConf: Configuration): FlinkStopResponse = {
    flinkConf.safeSet(YarnConfigOptions.APPLICATION_ID, stopRequest.extraParameter.get(KEY_YARN_APP_ID).toString)
    flinkConf.safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName)
    var clusterDescriptor: YarnClusterDescriptor = null
    var client: ClusterClient[ApplicationId] = null
    try {
      val yarnClusterDescriptor: (ApplicationId, YarnClusterDescriptor) = getYarnSessionClusterDescriptor(flinkConf)
      clusterDescriptor = yarnClusterDescriptor._2
      client = clusterDescriptor.retrieve(yarnClusterDescriptor._1).getClusterClient
      val jobID: JobID = JobID.fromHexString(stopRequest.jobId)
      val actionResult: String = cancelJob(stopRequest, jobID, client)
      FlinkStopResponse(actionResult)
    } catch {
      case exception: Exception=>
        throw exception
    } finally {
      if(null != client) client.close()
      if(null != clusterDescriptor) clusterDescriptor.close()
    }
  }


  override def query(queryRequest: FlinkQueryRequest): FlinkQueryResponse = {
    this.doQuery(queryRequest)
  }


  def deployYarnSession(flinkDeployRequest: FlinkDeployRequest): FlinkDeployResponse = {
    val flinkInfo: FlinkInfo = flinkDeployRequest.flinkInfo
    val flinkConfig: Configuration = this.getFlinkDefaultConfiguration(flinkInfo.flinkHome)
    flinkConfig.safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName)
    flinkConfig.safeSet(YarnConfigOptions.APPLICATION_NAME,flinkDeployRequest.clusterName)
    flinkConfig.safeSet(YarnConfigOptions.FLINK_DIST_JAR, flinkInfo.flinkDistJar.getPath)
    flinkConfig.safeSet(YarnConfigOptions.SHIP_FILES, flinkInfo.flinkYarnShipFiles.asJava)

    var clusterDescriptor: YarnClusterDescriptor = null
    var client: ClusterClient[ApplicationId] = null
    try {
      val serviceLoader = new DefaultClusterClientServiceLoader
      val clientFactory: ClusterClientFactory[ApplicationId] = serviceLoader.getClusterClientFactory[ApplicationId](flinkConfig)
      clusterDescriptor = clientFactory.createClusterDescriptor(flinkConfig).asInstanceOf[YarnClusterDescriptor]
      val clusterSpecification: ClusterSpecification = clientFactory.getClusterSpecification(flinkConfig)
      val clientProvider: ClusterClientProvider[ApplicationId] = clusterDescriptor.deploySessionCluster(clusterSpecification)
      client = clientProvider.getClusterClient
      FlinkDeployResponse(client.getWebInterfaceURL,client.getClusterId.toString)
    } catch {
      case exception: Exception =>
        throw exception
    } finally {
      if(null != client) client.close()
      if(null != clusterDescriptor) clusterDescriptor.close()
    }
  }


  def shutDownYarnSession(flinkShutdownRequest: FlinkShutdownRequest): FlinkShutdownResponse = {
    val flinkConfig: Configuration = this.getFlinkDefaultConfiguration(flinkShutdownRequest.flinkInfo.flinkHome)
    flinkConfig.safeSet(YarnConfigOptions.APPLICATION_ID, flinkShutdownRequest.clusterId)
    flinkConfig.safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName)

    var clusterDescriptor: YarnClusterDescriptor = null
    var client: ClusterClient[ApplicationId] = null

    try {
      val tuple: (ApplicationId, YarnClusterDescriptor) = getYarnSessionClusterDescriptor(flinkConfig)
      clusterDescriptor = tuple._2
      if (FinalApplicationStatus.UNDEFINED.equals(
        clusterDescriptor.getYarnClient.getApplicationReport(ApplicationId.fromString(flinkShutdownRequest.clusterId)).getFinalApplicationStatus)) {
        val provider: ClusterClientProvider[ApplicationId] = clusterDescriptor.retrieve(tuple._1)
        client = provider.getClusterClient
        client.shutDownCluster()
        return FlinkShutdownResponse(true)
      }
      FlinkShutdownResponse(false)
    } catch {
      case exception: Exception =>
        throw exception
    } finally {
      if(null != client) client.close()
      if(null != clusterDescriptor) clusterDescriptor.close()
    }
  }


  private[this] def getYarnSessionClusterDescriptor(flinkConfig: Configuration): (ApplicationId, YarnClusterDescriptor) = {
    val serviceLoader = new DefaultClusterClientServiceLoader
    val clientFactory: ClusterClientFactory[ApplicationId] = serviceLoader.getClusterClientFactory[ApplicationId](flinkConfig)
    val yarnClusterId: ApplicationId = clientFactory.getClusterId(flinkConfig)
    require(yarnClusterId != null)
    val clusterDescriptor: YarnClusterDescriptor =
      clientFactory.createClusterDescriptor(flinkConfig).asInstanceOf[YarnClusterDescriptor]
    (yarnClusterId, clusterDescriptor)
  }
}
