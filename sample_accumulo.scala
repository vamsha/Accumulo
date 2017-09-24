package com.db.accumulo

import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.accumulo.core.client.{BatchWriterConfig, Durability, Instance, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.hadoop.io.Text

/**
 * Hello world!
 *
 */
object sample_accumulo  {

  def main(args:Array[String]): Unit ={

    /*val usage = "run <instanceName> <zookeepers> <username> <password> <table>"
    args.length match {
      case 5 => println("using args:" + args.mkString(" "))
      case _ => {
        println(usage)
        return
      }
    }*/

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("spark-accumulo")
    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local")
      .getOrCreate()

    val instanceId = "accumulo"  //args(0)
    val zookeepers =  "zookeeper_hostname" //args(1)
    val user =  "username"  // args(2)
    val password =  "password"  //args(3)
    //"auths"      -> "USER,ADMIN"
    val table =  "spark_test"  //args(4)


    val inst = new ZooKeeperInstance(instanceId, zookeepers)

    val conn = inst.getConnector( user , new PasswordToken(password) )

    if(!conn.tableOperations().exists(table)){
      conn.tableOperations().create(table)
    }
    
    import org.apache.accumulo.core.security.Authorizations
    val auths = new Authorizations("public")

    //val scan = conn.createScanner("table", auths)

    val config:BatchWriterConfig = new BatchWriterConfig();
    config.setMaxLatency(1, TimeUnit.SECONDS)
    config.setMaxMemory(10240)
    config.setDurability(Durability.DEFAULT)
    config.setMaxWriteThreads(10)

    val batchWriter = conn.createBatchWriter(table, config)

    val mutation = new Mutation(new Text("row_id"))
    mutation.put(new Text("col_f"), new Text("col_q"), new Value("value".getBytes()))
    batchWriter.addMutation(mutation)
    batchWriter.close
    
    val scanner = conn.createScanner(table,Authorizations.EMPTY)
    import scala.collection.JavaConversions._
    val it = asScalaIterator(scanner.iterator).toSeq

    it.foreach {
      x => {
          println(x)
      }
    }

    println( "Hello World!" )

  }
}
