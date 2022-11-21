import com.czl.submitter.FlinkInfo;
import com.czl.submitter.FlinkSubmitter;
import com.czl.submitter.entity.FlinkSubmitRequest;
import com.czl.submitter.entity.FlinkSubmitResponse;
import com.czl.submitter.enums.ExecutionMode;
import com.czl.submitter.enums.ResolveOrder;
import org.apache.flink.configuration.RestOptions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/10/20
 * Description:
 */
public class FlinkSubmitTest {

    FlinkInfo flinkInfo = new FlinkInfo("/opt/flink-test");
    ResolveOrder resolveOrder = ResolveOrder.of(1);
    String appName = "flink-submit-test";
    String batchMainClass = "com.czl.submitter.FlinkTaskBatch";
//    String streamMainClass = "com.czl.submitter.FlinkTaskStream";
    String jarPath = "./jar/flink-task-api-test-1.0.jar";
    List<String> param = Arrays.asList("1","2","3");
    HashMap<String,Object> extraMap = new HashMap<>();


    @Test
    void localTest() {
        FlinkSubmitRequest localSubmit = new FlinkSubmitRequest(
                flinkInfo,
                ExecutionMode.LOCAL,
                resolveOrder,
                appName,
                batchMainClass,
                jarPath,
                null,
                1,
                param,
                extraMap);
        FlinkSubmitResponse submit = FlinkSubmitter.submit(localSubmit);
        System.out.println(submit.jobId());
    }


    @Test
    void remoteTest() throws URISyntaxException {
        ExecutionMode remoteMode = ExecutionMode.of(1);
        URI activeAddress = new URI("http://ip:port/");
        extraMap.put(RestOptions.ADDRESS.key(), activeAddress.getHost());
        extraMap.put(RestOptions.PORT.key(), activeAddress.getPort());
        FlinkSubmitRequest remoteSubmit = new FlinkSubmitRequest(
                flinkInfo,
                remoteMode,
                resolveOrder,
                appName,
                batchMainClass,
                jarPath,
                null,
                1,
                param,
                extraMap);
        FlinkSubmitResponse submit = FlinkSubmitter.submit(remoteSubmit);
        System.out.println(submit.jobId());
    }


    @Test
    void yarnSessionTest() {
        ExecutionMode yarnMode = ExecutionMode.of(2);
        extraMap.put("yarn.application.id", "application_1668191242058_0067");
        FlinkSubmitRequest yarnSessionSubmit = new FlinkSubmitRequest(
                flinkInfo,
                yarnMode,
                resolveOrder,
                appName,
                batchMainClass,
                jarPath,
                null,
                1,
                param,
                extraMap);
        FlinkSubmitResponse response = FlinkSubmitter.submit(yarnSessionSubmit);
        System.out.println(response.jobId());
    }


    @Test
    void yarnPerJobTest(){
        ExecutionMode yarnPerJob = ExecutionMode.of(3);
        FlinkSubmitRequest yarnSessionSubmit = new FlinkSubmitRequest(
                flinkInfo,
                yarnPerJob,
                resolveOrder,
                appName,
                batchMainClass,
                jarPath,
                null,
                1,
                param,
                extraMap);
        FlinkSubmitResponse response = FlinkSubmitter.submit(yarnSessionSubmit);
        System.out.println(response.clusterId());
        System.out.println(response.jobId());
    }
}
