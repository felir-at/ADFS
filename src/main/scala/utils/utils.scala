package adfs

import java.util.concurrent.ThreadLocalRandom

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

  case object NormalDistribution {
    def nextGaussian(mean: Double, deviation: Double) = deviation.abs * ThreadLocalRandom.current().nextGaussian() + mean
  }


  /** returns the median of the sorted sample
    *
    * This median is different from the usual definition. In case of an empty sample it returns None, otherwise in
    * case of odd number of samples it returns the middle element, and in case of an even number of samples it
    * returns the (N/2-1)th element (N is the sample size).
    *
    * @param sample
    */
  def median(sample: Seq[Option[Int]]) = {
    val size = sample.size

    if( size == 0 ){
      None
    }else if( size % 2 == 1 ){
      sample( size / 2 )
    }else{
      sample( size / 2 - 1 )
    }
  }
}
