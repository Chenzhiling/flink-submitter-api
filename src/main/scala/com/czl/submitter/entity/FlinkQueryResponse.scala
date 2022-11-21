package com.czl.submitter.entity

/**
 * Author: CHEN ZHI LING
 * Date: 2022/10/21
 * Description:
 */
case class FlinkQueryResponse(jobId: String,
                              name: String,
                              state: String,
                              startTime: Long,
                              endTime: Long,
                              duration: Long)
