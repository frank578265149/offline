
import bigdata.analysis.scala.dao.Getip
import org.scalatest.{FunSuite, Matchers}

/**
  * Created by frank on 17-2-16.
  */
class GetIpTest extends FunSuite with Matchers {
  test(">>>>>>>>>>>>>>>>>>>"){
   val str= Getip.getContent("114.115.213.107")
    println(str)
  }
}
