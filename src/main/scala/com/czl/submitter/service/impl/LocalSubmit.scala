package com.czl.submitter.service.impl

import com.czl.submitter.entity.{FlinkQueryRequest, FlinkQueryResponse, FlinkStopRequest, FlinkStopResponse, FlinkSubmitRequest, FlinkSubmitResponse}
import com.czl.submitter.service.FlinkSubmitTrait
import org.apache.flink.client.deployment.executors.RemoteExecutor
import org.apache.flink.client.program.MiniClusterClient.MiniClusterId
import org.apache.flink.client.program.{ClusterClient, MiniClusterClient, PackagedProgram}
import org.apache.flink.configuration._
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}

import java.lang.{Integer => JavaInt}

/**
 * Author: CHEN ZHI LING
 * Date: 2022/6/8
 * Description:
 */
object LocalSubmit extends FlinkSubmitTrait {


  override def setConfig(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): Unit = {
    //flink job name
    flinkConf.safeSet(PipelineOptions.NAME, submitRequest.effectiveAppName)
  }


  override def doSubmit(submitRequest: FlinkSubmitRequest, flinkConf: Configuration): FlinkSubmitResponse = {
    var packageProgram: PackagedProgram = null
    var client: ClusterClient[MiniClusterId] = null
    try {
      val packageProgramJobGraph: (PackagedProgram, JobGraph) =
        super.getJobGraph(flinkConf, submitRequest, submitRequest.supportTaskJarFile)
      packageProgram = packageProgramJobGraph._1
      val jobGraph: JobGraph = packageProgramJobGraph._2
      //create local miniCluster
      client = createLocalCluster(flinkConf)
      val jobId: String = client.submitJob(jobGraph).get().toString
      val result: FlinkSubmitResponse = FlinkSubmitResponse(client.getClusterId.toString, jobId)
      result
    } catch {
      case exception: Exception =>
        throw exception
    } finally {
      if(null != packageProgram) packageProgram.close()
      if(null != client) client.close()
    }
  }


  override def doStop(stopRequest: FlinkStopRequest, flinkConf: Configuration): FlinkStopResponse = {
    RemoteSubmit.doStop(stopRequest, flinkConf)
  }


  override def query(queryRequest: FlinkQueryRequest): FlinkQueryResponse = {
    FlinkQueryResponse("","","",0,0,0)
  }


  private[this] def createLocalCluster(flinkConfig: Configuration): MiniClusterClient = {
    val miniCluster: MiniCluster = createMiniCluster(flinkConfig)
    val host: String = "localhost"
    val port: Int = miniCluster.getRestAddress.get.getPort
    flinkConfig
      .safeSet(JobManagerOptions.ADDRESS,host)
      .safeSet[JavaInt](JobManagerOptions.PORT,port)
      .safeSet(RestOptions.ADDRESS, host)
      .safeSet[JavaInt](RestOptions.PORT, port)
      .safeSet(DeploymentOptions.TARGET, RemoteExecutor.NAME)
    new MiniClusterClient(flinkConfig,miniCluster)

  }


  private[this] def createMiniCluster(flinkConfig: Configuration): MiniCluster = {
    val numTaskManagers: Int = ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER
    val numSlotsPerTaskManager: Int = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS)
    val miniClusterConfig: MiniClusterConfiguration =
      new MiniClusterConfiguration.Builder()
        .setConfiguration(flinkConfig)
        .setNumTaskManagers(numTaskManagers)
        .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
        .build()
    val cluster = new MiniCluster(miniClusterConfig)
    cluster.start()
    cluster
  }
}
