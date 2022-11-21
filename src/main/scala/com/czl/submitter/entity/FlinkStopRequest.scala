package com.czl.submitter.entity

import com.czl.submitter.FlinkInfo
import com.czl.submitter.enums.ExecutionMode

import java.util.{Map => JavaMap}

/**
 * Author: CHEN ZHI LING
 * Date: 2022/10/21
 * Description:
 */
case class FlinkStopRequest(flinkVersion: FlinkInfo,
                            executionMode: ExecutionMode,
                            clusterId: String,
                            jobId: String,
                            withSavePoint: Boolean,
                            customSavePointPath: String,
                            withDrain: Boolean,
                            extraParameter: JavaMap[String, Any])
