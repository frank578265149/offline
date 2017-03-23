package bigdata.analysis.scala.hbase

import org.scalatest.FunSuite

/**
  * Created by frank on 17-1-6.
  */
class RegexSuite extends  FunSuite{

  test(s" find first in "){
    val pattern="Scala".r
    val str="Scala is Scalable and cool`"
    println(pattern findFirstIn(str))
  }
  
}
