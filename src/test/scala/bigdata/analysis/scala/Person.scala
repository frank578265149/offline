package bigdata.analysis.scala

import java.io.Serializable

/**
  * Created by frank on 17-3-16.
  */
class Person(name:String,sex:String) extends  Serializable{

  def sayHello():Unit={
    println("sayhello")
  }
}
