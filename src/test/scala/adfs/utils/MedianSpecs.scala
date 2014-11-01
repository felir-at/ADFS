package adfs.utils.test


import adfs.utils._
import org.scalatest.{WordSpecLike, BeforeAndAfterAll, Matchers}

/**
 * Created by kosii on 2014. 11. 01..
 */
class MedianSpecs extends WordSpecLike with Matchers with BeforeAndAfterAll {

  "median" should {

    "return None for empty sample" in {
      median(Seq[Option[Int]]()) should be { None }
    }

    "correctly determine median in sorted samples" in {
      median(Seq[Option[Int]](Some(1), Some(2), Some(3))) should be { Some(2) }
      median(Seq[Option[Int]](Some(1), Some(2), Some(3), Some(4))) should be { Some(2) }
      median(Seq[Option[Int]](Some(1), Some(2), Some(3), Some(4))) should be { Some(2) }
      median(Seq[Option[Int]](Some(2), Some(2), Some(2))) should be { Some(2) }
    }

    "not work on unsorted samples" in {
      median(Seq[Option[Int]](Some(1), Some(3), Some(2))) should not be { Some(2) }
      median(Seq[Option[Int]](Some(1), Some(4), Some(2), Some(3))) should not be { Some(2) }
    }

  }

}
