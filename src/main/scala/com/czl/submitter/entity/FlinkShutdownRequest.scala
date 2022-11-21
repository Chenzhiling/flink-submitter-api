package com.czl.submitter.entity

import com.czl.submitter.FlinkInfo

/**
 * Author: CHEN ZHI LING
 * Date: 2022/11/18
 * Description:
 */
case class FlinkShutdownRequest(flinkInfo: FlinkInfo,
                                clusterId: String)
