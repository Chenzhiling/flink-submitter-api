package com.czl.submitter.service

import org.apache.flink.client.deployment.ClusterSpecification
import org.apache.flink.client.program.ClusterClientProvider
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.yarn.YarnClusterDescriptor
import org.apache.hadoop.yarn.api.records.ApplicationId

import java.lang.reflect.Method
import java.lang.{Boolean => JavaBool}

/**
 * Author: CHEN ZHI LING
 * Date: 2022/10/27
 * Description:
 */
trait FlinkYarnSubmitTrait extends FlinkSubmitTrait {


  lazy private val deployInternalMethod: Method = {
    val paramClass: Array[Class[_]] = Array(
      classOf[ClusterSpecification],
      classOf[String],
      classOf[String],
      classOf[JobGraph],
      Boolean2boolean(true).getClass
    )
    val deployInternal: Method = classOf[YarnClusterDescriptor].getDeclaredMethod("deployInternal", paramClass: _*)
    deployInternal.setAccessible(true)
    deployInternal
  }


  private[submitter] def deployInternal(clusterDescriptor: YarnClusterDescriptor,
                                     clusterSpecification: ClusterSpecification,
                                     applicationName: String,
                                     yarnClusterEntrypoint: String,
                                     jobGraph: JobGraph,
                                     detached: JavaBool): ClusterClientProvider[ApplicationId] = {
    deployInternalMethod.invoke(
      clusterDescriptor,
      clusterSpecification,
      applicationName,
      yarnClusterEntrypoint,
      jobGraph,
      detached
    ).asInstanceOf[ClusterClientProvider[ApplicationId]]
  }
}
