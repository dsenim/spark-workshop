package com.tikal.spark.workshop.stream

import akka.actor.{Actor, Props}
import org.apache.spark.streaming._
import org.apache.spark.{Logging, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.{ActorHelper, Receiver}

import scala.util.{Random, Try}

/**
 * CustomReceiverStream 
 *
 * @author Dmitri Krasnenko
 */

class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  override def onStart(): Unit = {
    new Thread("My-Cool-Receiver"){
      override def run(): Unit = {
         receive()
      }
    }.start()

  }

  override def onStop(): Unit = {

  }

  def receive(): Unit = {
      while(true){
        val v = Random.nextInt(500).toString
        store(v)

        Try(Thread.sleep(1000))
      }
  }
}

object CustomReceiverStream {
  def main(args: Array[String]) {
    val ssc = new StreamingContext(new SparkConf().setMaster("local[4]").setAppName("my-cool-receiver-stream"), Seconds(10))

    ssc.receiverStream(new CustomReceiver()).print()

    ssc.start()
    ssc.awaitTermination()

  }
}

class CustomActor[String] extends Actor with ActorHelper {
  //Dummy sender thread. Don't do it in production, use another actor instead.
  new Thread("My-Cool-Sender"){
    override def run(): Unit = {
      while (true){
        self ! Random.nextInt(500).toString
        Try(Thread.sleep(1000))
      }
    }
  }.start()

  override def receive: Receive = {
    case message: String => {
      store(message)
    }
  }
}

object CustomActorStream {
  def main(args: Array[String]) {
    val ssc = new StreamingContext(new SparkConf().setMaster("local[4]").setAppName("my-cool-actor-stream"), Seconds(10))

    ssc.actorStream[String](Props(new CustomActor()), "My-Cool-Actor").print()

    ssc.start()
    ssc.awaitTermination()

  }
}
