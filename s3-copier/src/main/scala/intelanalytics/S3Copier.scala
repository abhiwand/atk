import awscala._, s3._
import com.intel.intelanalytics.aws.ProxyS3Client
package com.intel.intelanalytics {

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.{Protocol, ClientConfiguration}

//TODO: LICENSE
object S3Copier {
  def main(args: Array[String]) {

    val config = new ClientConfiguration()
                      .withProxyHost("proxy.jf.intel.com")
                      .withProxyPort(912)
                      .withProtocol(Protocol.HTTPS)
    println("Creating S3 object")
    val client = new ProxyS3Client(Credentials.defaultEnv, config)
    implicit val s3 = client // S3()

    println("Getting gaopublic bucket")
    s3.bucket("gaopublic").map {
      _.keys.map(println)
    }
    println("Done")
    //val bucket: Bucket = s3.createBucket("unique-name-xxx")
    //val summaries: Seq[S3ObjectSummary] = bucket.objectSummaries

    //bucket.put("sample.txt", new java.io.File("sample.txt"))

    //val s3obj: Option[S3Object] = bucket.getObject("sample.txt")

    // s3obj.foreach { obj =>
    //obj.publicUrl // http://unique-name-xxx.s3.amazonaws.com/sample.txt
    //obj.get.generatePresignedUrl(DateTime.now.plusMinutes(10)) // ?Expires=....
    //bucket.delete(obj) // or obj.destroy()
    // }
  }
}

}
