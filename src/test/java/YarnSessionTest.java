import com.czl.submitter.FlinkInfo;
import com.czl.submitter.entity.FlinkDeployRequest;
import com.czl.submitter.entity.FlinkDeployResponse;
import com.czl.submitter.entity.FlinkShutdownRequest;
import com.czl.submitter.service.impl.YarnSessionSubmit;
import org.junit.jupiter.api.Test;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/11/21
 * Description:
 */
public class YarnSessionTest {

    @Test
    void deploy(){
        FlinkInfo flinkInfo = new FlinkInfo("/opt/flink-test");
        FlinkDeployRequest deployRequest = new FlinkDeployRequest(flinkInfo, "czl-yarn-session-test");
        FlinkDeployResponse flinkDeployResponse = YarnSessionSubmit.deployYarnSession(deployRequest);
        System.out.println(flinkDeployResponse.address());
        System.out.println(flinkDeployResponse.clusterId());
    }


    @Test
    void shutdown(){
        FlinkInfo flinkInfo = new FlinkInfo("/opt/flink-test");
        FlinkShutdownRequest shutdownRequest = new FlinkShutdownRequest(flinkInfo, "application_1668191242058_0072");
        YarnSessionSubmit.shutDownYarnSession(shutdownRequest);
    }
}
