import com.typesafe.config.{ConfigFactory, Config}

/**
 * Created by kosii on 2014.09.20..
 */


package object utils {
  def remoteConfig(hostname: String, port: Int, commonConfig: Config = ConfigFactory.load()): Config = {
    val configStr =
      "akka.remote.netty.tcp.hostname = " + hostname + "\n" +
        "akka.remote.netty.tcp.port = " + port + "\n"

    ConfigFactory.parseString(configStr).withFallback(commonConfig)
  }


}
