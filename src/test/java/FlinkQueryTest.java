import com.czl.submitter.FlinkSubmitter;
import com.czl.submitter.entity.FlinkQueryRequest;
import com.czl.submitter.entity.FlinkQueryResponse;
import com.czl.submitter.enums.ExecutionMode;
import org.junit.jupiter.api.Test;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/10/21
 * Description:
 */
public class FlinkQueryTest {


    @Test
    void remoteQuery() {
        String master = "http://ip:port";
        String jobId = "e0641a4f9f060961419531dd5233fe6c";
        FlinkQueryRequest request = new FlinkQueryRequest(ExecutionMode.REMOTE, master, jobId);
        FlinkQueryResponse query = FlinkSubmitter.query(request);
        System.out.println(query);
    }


    @Test
    void yarnSessionQuery() {
        String master = "http://masterchen:8088/proxy/application_1668191242058_0067/";
        String jobId = "49ec439ec9739bc9fd30aa59cecacddf";
        FlinkQueryRequest request = new FlinkQueryRequest(ExecutionMode.FLINK_YARN_SESSION, master, jobId);
        FlinkQueryResponse query = FlinkSubmitter.query(request);
        System.out.println(query);
    }


    @Test
    void yarnPerJobQuery() {
        String master = "http://masterchen:8088/proxy/application_1668191242058_0066/";
        String jobId = "e60405138135c600e98536b993dcde98";
        FlinkQueryRequest request = new FlinkQueryRequest(ExecutionMode.FLINK_YARN_PER_JOB, master, jobId);
        FlinkQueryResponse query = FlinkSubmitter.query(request);
        System.out.println(query);
    }
}
