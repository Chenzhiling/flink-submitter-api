import com.czl.submitter.FlinkInfo;
import com.czl.submitter.FlinkSubmitter;
import com.czl.submitter.consts.FlinkConst;
import com.czl.submitter.entity.FlinkStopRequest;
import com.czl.submitter.entity.FlinkStopResponse;
import com.czl.submitter.enums.ExecutionMode;
import org.apache.flink.configuration.RestOptions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/11/21
 * Description:
 */
public class FlinkStopTest {

    FlinkInfo flinkInfo = new FlinkInfo("/opt/flink-test");
    String customSavePoint = "./sp/";
    Map<String, Object> extraParameter = new HashMap<>(0);

    @Test
    void remoteStop() throws URISyntaxException {
        ExecutionMode remote = ExecutionMode.REMOTE;
        URI activeAddress = new URI("http://ip:port/");
        extraParameter.put(RestOptions.ADDRESS.key(), activeAddress.getHost());
        extraParameter.put(RestOptions.PORT.key(), activeAddress.getPort());
        FlinkStopRequest request = new FlinkStopRequest(flinkInfo,
                remote,
                null,
                "e0641a4f9f060961419531dd5233fe6c",
                true,
                customSavePoint,
                false, extraParameter);
        FlinkStopResponse stop = FlinkSubmitter.stop(request);
        System.out.println(stop.savePointPath());
    }


    @Test
    void yarnSessionStop() {
        ExecutionMode yarnSession = ExecutionMode.FLINK_YARN_SESSION;
        String jobId = "1373043f33b3483c535214a7ec572993";
        extraParameter.put(FlinkConst.KEY_YARN_APP_ID(),"application_1668191242058_0067");
        FlinkStopRequest request = new FlinkStopRequest(flinkInfo,
                yarnSession,
                null,
                jobId,
                true,
                customSavePoint,
                false, extraParameter);
        FlinkStopResponse stop = FlinkSubmitter.stop(request);
        System.out.println(stop.savePointPath());
    }


    @Test
    void yarnPerJobStop() {
        ExecutionMode yarnPerJob = ExecutionMode.FLINK_YARN_PER_JOB;
        String clusterId = "application_1668191242058_0070";
        String jobId = "1373043f33b3483c535214a7ec572993";
        FlinkStopRequest request = new FlinkStopRequest(flinkInfo,
                yarnPerJob,
                clusterId,
                jobId,
                true,
                customSavePoint,
                false, extraParameter);
        FlinkStopResponse stop = FlinkSubmitter.stop(request);
        System.out.println(stop.savePointPath());
    }
}
