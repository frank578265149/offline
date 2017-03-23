package bigdata.analysis.scala.dao.jdbc

import scala.language.existentials


case class  StorageClientConfig(
                               parallel:Boolean=false,
                               test:Boolean=false,
                               properties:Map[String,String]=Map()
                               )




