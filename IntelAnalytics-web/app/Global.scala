import com.typesafe.config.ConfigFactory
import play.api.mvc._
import play.api._
import java.io.File
import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.Some
import scala.Some
import play.api.Play.current
import services.aws.S3


object Global extends GlobalSettings{

  override def onLoadConfig(config: Configuration, path: File, classloader: ClassLoader, mode: Mode.Mode): Configuration = {
    //if we get a play mode environment variable use that instead of the default one from play
    val playMode = if(System.getProperty("play.config") == null) mode.toString.toLowerCase else System.getProperty("play.config").toLowerCase
    val customConfigFileName = s"application.${playMode}.conf";
    //load our config
    val loadConfig = ConfigFactory.load(customConfigFileName)

    val customConfigs = loadConfig.withOnlyPath(playMode.toLowerCase).entrySet map (_.getKey)
    for(customConfig <- customConfigs){
      System.setProperty(customConfig.replace(playMode + ".", ""), loadConfig.getString(customConfig))
    }

    val modeSpecificConfig = config ++ Configuration(loadConfig)
    if(!playMode.eq(Mode.Prod.toString.toLowerCase)){
      System.out.println("loaded config file: " + customConfigFileName)
    }

    //pass the regular play mode since our own custom mode is only use for loading a config
    super.onLoadConfig(modeSpecificConfig, path, classloader, mode)
  }

  override def onStart(app : play.api.Application){
    //create s3 bucket to hold user files
    S3.createBucket()
  }

  /*try this on prod with elb infront of it
  override def onRouteRequest(request: RequestHeader): Option[Handler] = {
    if (Play.isDev && !request.headers.get("x-forwarded-proto").getOrElse("").contains("https")) {
      Some(controllers.Application.redirect)
    } else {
      super.onRouteRequest(request)
    }
  }*/

}
