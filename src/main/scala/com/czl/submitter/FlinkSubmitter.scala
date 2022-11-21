package com.czl.submitter

import com.czl.submitter.entity._
import com.czl.submitter.enums.ExecutionMode
import com.czl.submitter.service.impl.{LocalSubmit, RemoteSubmit, YarnPerJobSubmit, YarnSessionSubmit}

/**
 * Author: CHEN ZHI LING
 * Date: 2022/5/26
 * Description:
 */
object FlinkSubmitter {

  /**
   * submit flink task
   */
  def submit(submitRequest: FlinkSubmitRequest): FlinkSubmitResponse = {
    submitRequest.executionMode match {
      case ExecutionMode.LOCAL =>   LocalSubmit.submit(submitRequest)
      case ExecutionMode.REMOTE =>  RemoteSubmit.submit(submitRequest)
      case ExecutionMode.FLINK_YARN_SESSION => YarnSessionSubmit.submit(submitRequest)
      case ExecutionMode.FLINK_YARN_PER_JOB => YarnPerJobSubmit.submit(submitRequest)
      case _ => throw new UnsupportedOperationException(s"Unsupported ${submitRequest.executionMode} mode to submit flink task")
    }
  }

  /**
   * query flink task
   */
  def query(queryRequest: FlinkQueryRequest): FlinkQueryResponse = {
    queryRequest.executionMode match {
      case ExecutionMode.LOCAL  => LocalSubmit.query(queryRequest)
      case ExecutionMode.REMOTE => RemoteSubmit.query(queryRequest)
      case ExecutionMode.FLINK_YARN_SESSION => YarnSessionSubmit.query(queryRequest)
      case ExecutionMode.FLINK_YARN_PER_JOB => YarnPerJobSubmit.query(queryRequest)
      case _ => throw new UnsupportedOperationException(s"Unsupported ${queryRequest.executionMode} mode to query flink task")
    }
  }

  /**
   * stop flink task
   */
  def stop(stopRequest: FlinkStopRequest): FlinkStopResponse = {
    stopRequest.executionMode match {
      case ExecutionMode.LOCAL => LocalSubmit.stop(stopRequest)
      case ExecutionMode.REMOTE => RemoteSubmit.stop(stopRequest)
      case ExecutionMode.FLINK_YARN_SESSION => YarnSessionSubmit.stop(stopRequest)
      case ExecutionMode.FLINK_YARN_PER_JOB => YarnPerJobSubmit.stop(stopRequest)
      case _ => throw new UnsupportedOperationException(s"Unsupported ${stopRequest.executionMode} node to stop flink task")
    }
  }

  /**
   * deploy flink-yarn-session cluster
   */
  def deploy(flinkDeployRequest: FlinkDeployRequest): FlinkDeployResponse = {
    YarnSessionSubmit.deployYarnSession(flinkDeployRequest)
  }

  /**
   * shutdown flink-yarn-session cluster
   */
  def shutDown(flinkShutdownRequest: FlinkShutdownRequest): FlinkShutdownResponse = {
    YarnSessionSubmit.shutDownYarnSession(flinkShutdownRequest)
  }
}
