package com.intel.intelanalytics.aws

import awscala.Credentials
import awscala.s3.{Bucket, S3}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.ClientConfiguration

//TODO: LICENSE
class ProxyS3Client(credentials: Credentials = Credentials.defaultEnv,
                     configuration: ClientConfiguration)
    extends AmazonS3Client(credentials, configuration)
    with S3 {

    override def createBucket(name: String): Bucket = super.createBucket(name)
  }
