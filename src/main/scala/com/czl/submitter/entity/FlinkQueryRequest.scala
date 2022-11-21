package com.czl.submitter.entity

import com.czl.submitter.enums.ExecutionMode

/**
 * Author: CHEN ZHI LING
 * Date: 2022/10/21
 * Description:
 */
case class FlinkQueryRequest(executionMode: ExecutionMode,
                             master: String,
                             jobId: String)
