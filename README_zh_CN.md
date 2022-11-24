# flink任务提交器

[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](README_zb_CN.md)
[![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.md)
## 1. 简介

因项目，需要在spring boot后台项目中集成flink任务提交，查询之类的功能，所有有了这个项目

这个项目，可以通过java api的形式，帮助你提交，查询，暂停flink任务，也可以构建和关闭flink yarn session集群。

## 2. 支持以下的Flink运行模式

- Flink local
- Flink remote

- Flink yarn session
- Flink yarn per job

## 3. 主要类

### 3.1 FlinkInfo

根据Flink运行环境路径地址生成，提交和停止任务时使用

```scala
class FlinkInfo(val flinkHome: String) extends Serializable {



  private[this] lazy val FLINK_SCALA_VERSION_PATTERN: Pattern = Pattern.compile("^flink-dist_(.*)-(.*).jar$")


  lazy val fullVersion: String = s"${version}_$scalaVersion"


  /**
   * get scala version
   */
  lazy val scalaVersion: String = {
    val matcher: Matcher = FLINK_SCALA_VERSION_PATTERN.matcher(flinkDistJar.getName)
    matcher.matches()
    matcher.group(1)
  }


  /**
   * get Flink version
   */
  lazy val version: String = {
    val matcher: Matcher = FLINK_SCALA_VERSION_PATTERN.matcher(flinkDistJar.getName)
    matcher.matches()
    matcher.group(2)
  }


  /**
   * get all flink libs
   */
  lazy val flinkLib: File = {
    require(flinkHome != null, "flinkHome can not be null")
    require(new File(flinkHome).exists(), "flinkHome not exist")
    val lib = new File(s"$flinkHome/lib")
    require(lib.exists() && lib.isDirectory, s"$flinkHome/lib should be directory")
    lib
  }


  /**
   * yarn-ship content
   */
  lazy val flinkYarnShipFiles: List[String] = {
    List(flinkLib.toString,s"$flinkHome/plugins",s"$flinkHome/conf/log4j.properties")
  }


  /**
   * get flink distJar
   */
  lazy val flinkDistJar: File = {
    val distJar: Array[File] = flinkLib.listFiles().filter((_: File).getName.matches("flink-dist_.*\\.jar"))
    distJar match {
      case x if x.isEmpty =>
        throw new IllegalArgumentException(s"no flink-dist.jar in $flinkLib")
      case x if x.length > 1 =>
        throw new IllegalArgumentException(s"there are multiple flink-dist.jar in $flinkLib ")
      case _ =>
    }
    distJar.head
  }
}
```

### 3.2 ExecutionMode

执行模式枚举类

```java
public enum ExecutionMode implements Serializable {


    LOCAL(0, "local"),

    REMOTE(1, "remote"),

    FLINK_YARN_SESSION(2, "yarn-session"),

    FLINK_YARN_PER_JOB(3,"yarn-per-job");

    private final Integer mode;

    private final String name;

    ExecutionMode(Integer mode, String name) {
        this.mode = mode;
        this.name = name;
    }
}
```

### 3.3 ResolveOrder

类加载策略枚举类

```java
public enum ResolveOrder {
    /**
     * parent-first
     */
    PARENT_FIRST("parent-first", 0),
    /**
     * child-first
     */
    CHILD_FIRST("child-first", 1);

    private final String name;

    private final Integer value;

    ResolveOrder(String name, Integer value) {
        this.name = name;
        this.value = value;
    }
}
```

### 3.4 FlinkSubmitRequest

用于提交Flink任务

- appName 任务名称

- flinkJarPath jar包地址
- savePoint 检查的路径
- flinkParallelism 并行度
- args  参数集合

```scala
case class FlinkSubmitRequest(flinkVersion: FlinkInfo,
                              executionMode: ExecutionMode,
                              resolveOrder: ResolveOrder,
                              appName: String,
                              mainClass: String,
                              flinkJarPath: String,
                              savePoint: String,
                              flinkParallelism: Int,
                              args: JavaList[String],
                              extraParameter: JavaMap[String, Any]) {


  lazy val effectiveAppName: String = if (this.appName == null) "flink-task" else this.appName


  lazy val supportTaskJarFile: File = {
        new File(flinkJarPath)
  }


  lazy val savepointRestoreSettings: SavepointRestoreSettings = {
    lazy val allowNonRestoredState: Boolean = Try(
      extraParameter.get(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key).toString.toBoolean).getOrElse(false)
    savePoint match {
      case sp if Try(sp.isEmpty).getOrElse(true) => SavepointRestoreSettings.none
      case sp => SavepointRestoreSettings.forPath(sp, allowNonRestoredState)
    }
  }
}
```



## 4.测试用例(remote模式)

其余模式测试用例请参考工程里的代码

### 4.1 提交flink任务

```java
public class FlinkSubmitTest {
    FlinkInfo flinkInfo = new FlinkInfo("/opt/flink-test");
    ResolveOrder resolveOrder = ResolveOrder.of(1);
    String appName = "flink-submit-test";
    String batchMainClass = "com.czl.submitter.FlinkTaskBatch";
    String jarPath = "./jar/flink-task-api-test-1.0.jar";
    List<String> param = Arrays.asList("1","2","3");
    HashMap<String,Object> extraMap = new HashMap<>();
    
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
}
```

### 4.2 查询flink任务状态

主要参数

- 执行模式
- flink运行地址
- jobId



```java
public class FlinkQueryTest {
    @Test
    void remoteQuery() {
        String master = "http://ip:port";
        String jobId = "e0641a4f9f060961419531dd5233fe6c";
        FlinkQueryRequest request = new FlinkQueryRequest(ExecutionMode.REMOTE, master, jobId);
        FlinkQueryResponse query = FlinkSubmitter.query(request);
        System.out.println(query);
    }
}

```

### 4.3 停止flink任务

主要参数

- savepoint路径
- jobId

```java
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
}
```

### 4.4 部署和停止flink yarn session集群

主要参数

- FlinkInfo
- yarn name

```java
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
```

## 5. 仓库地址

(https://github.com/Chenzhiling/flink-submitter-api)

## 6. 参考

从StreamX项目获取相当多的思路，感谢