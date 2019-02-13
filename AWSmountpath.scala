object MyAws {
val secret = "*****".replace("/", "%2F")
val key = "A*****"
val myBucket = "com.harish.sample"
val MountName = "parking_citation"
lazy val parking_citation = s"s3a://${key}:${secret}@s3/buckets/${myBucket}/${MountName}"
}

// Mount aws path

dbutils.fs.mount(s"s3a://${MyAws.key}:${MyAws.secret}@${MyAws.myBucket}",s"/mnt/${MyAws.MountName}")

