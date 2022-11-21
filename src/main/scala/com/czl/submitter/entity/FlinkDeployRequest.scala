package com.czl.submitter.entity

import com.czl.submitter.FlinkInfo

/**
 * Author: CHEN ZHI LING
 * Date: 2022/11/17
 * Description:
 */
case class FlinkDeployRequest(flinkInfo: FlinkInfo,
                              clusterName: String)
