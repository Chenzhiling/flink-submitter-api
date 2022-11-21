package com.czl.submitter.service.tool

import com.czl.submitter.entity.FlinkQueryResponse
import org.apache.flink.client.deployment.application.ApplicationConfiguration
import org.apache.flink.configuration.{Configuration, CoreOptions}
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder
import org.apache.hc.client5.http.fluent.Request
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.core5.util.Timeout
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import java.io.File
import java.nio.charset.StandardCharsets
import java.util
import scala.util.{Failure, Success, Try}

/**
 * Author: CHEN ZHI LING
 * Date: 2022/5/27
 * Description: submit flink jar by restful api
 */
object FlinkSubmitUtils {


  @transient
  implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats


  def submitJarViaRestApi(jobManagerUrl: String,flinkJobJar: File, flinkConfig: Configuration): String = {
    // return "{"filename":"/tmp/xx.jar","status":"success"}"
    val uploadResult: String = Request.post(s"$jobManagerUrl/jars/upload")
      .connectTimeout(Timeout.ofSeconds(10))
      .responseTimeout(Timeout.ofSeconds(60))
      .body(MultipartEntityBuilder
        .create()
        .addBinaryBody("jarFile", flinkJobJar, ContentType.create("application/java-archive"), flinkJobJar.getName)
        .build())
      .execute()
      .returnContent()
      .asString(StandardCharsets.UTF_8)
    //upload jar
    val jarUploadResponse: JarUploadResponse = Try(parse(uploadResult)) match {
      case Success(ok) =>
        JarUploadResponse(
          (ok \ "filename").extractOpt[String].orNull,
          (ok \ "status").extractOpt[String].orNull
        )
      case Failure(_) => null
    }
    if (!jarUploadResponse.isSuccessful) {
      throw new Exception("flink-submit failed")
    }
    val response: String = Request.post(s"$jobManagerUrl/jars/${jarUploadResponse.jarId}/run")
      .connectTimeout(Timeout.ofSeconds(10))
      .responseTimeout(Timeout.ofSeconds(60))
      //entry-class=mainClass&parallelism=1&args=[...]
      .body(new StringEntity(Serialization.write(new JarRunRequest(flinkConfig))))
      .execute()
      .returnContent()
      .asString(StandardCharsets.UTF_8)
    //ret jobId
    Try(parse(response)) match {
      case Success(ok) => (ok \ "jobid").extractOpt[String].orNull
      case Failure(_) => null
    }
  }


  def queryTaskViaRestApi(url: String): FlinkQueryResponse = {
    val response: String = Request.get(url)
      .connectTimeout(Timeout.ofSeconds(10))
      .responseTimeout(Timeout.ofSeconds(60))
      .execute()
      .returnContent()
      .asString(StandardCharsets.UTF_8)
    val queryResponse: FlinkQueryResponse = Try(parse(response)) match {
      case Success(ok) =>
        FlinkQueryResponse(
          (ok \ "jid").extractOpt[String].get,
          (ok \ "name").extractOpt[String].get,
          (ok \ "state").extractOpt[String].get,
          (ok \ "start-time").extractOpt[Long].get,
          (ok \ "end-time").extractOpt[Long].get,
          (ok \ "duration").extractOpt[Long].get
        )
      case Failure(_) => null
    }
    queryResponse
  }
}

private[submitter] case class JarUploadResponse(filename: String, status: String) {

  def isSuccessful: Boolean = "success".equalsIgnoreCase(status)
  // return like ba4a07bd-0ecf-4ded-8e69-da201f62385f_filename.jar
  def jarId: String = filename.substring(filename.lastIndexOf("/") + 1)
}

/**
 * @example http://10.88.10.44:8081/jars/91ab0728-1156-4c65-a769-40ec143937dc_1.jar/run?entry-class=com.czl.study.flinkTest&parallelism=1
 */
private[submitter] case class JarRunRequest(entryClass: String,
                                         programArgs: String,
                                         parallelism: String,
                                         savepointPath: String,
                                         allowNonRestoredState: Boolean) {
  def this(flinkConf: Configuration) {
    this(
      entryClass = flinkConf.get(ApplicationConfiguration.APPLICATION_MAIN_CLASS),
      programArgs = Option(flinkConf.get(ApplicationConfiguration.APPLICATION_ARGS))
        .map(String.join(" ", _: util.List[String])).orNull,
      parallelism = String.valueOf(flinkConf.get(CoreOptions.DEFAULT_PARALLELISM)),
      savepointPath = flinkConf.get(SavepointConfigOptions.SAVEPOINT_PATH),
      allowNonRestoredState = flinkConf.getBoolean(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE)
    )
  }
}
