import com.typesafe.config.ConfigFactory
import play.api.mvc._
import play.api._
import java.io.File
import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.Some
import play.api.Play.current
import services.aws.S3
import play.api.http.HeaderNames._
import ExecutionContext.Implicits.global


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
    //don't create the s3 bucket in test mode
    if(!Play.isTest){
      //create s3 bucket to hold user files
      S3.createBucket()
    }
  }

  override def doFilter(action: EssentialAction): EssentialAction = EssentialAction { request =>
    action.apply(request).map(_.withHeaders(
      "X-Frame-Options" -> "SAMEORIGIN"
    ))
  }

  override def onRouteRequest(request: RequestHeader): Option[Handler] = {
    //check if the aws health checker is hitting the server
    val elbHealthChecker = request.headers.get(USER_AGENT).getOrElse("").contains("ELB-HealthChecker")

    //only force https on prod mode when User-Agent header is anything but aws health check
    if (Play.isProd &&  !elbHealthChecker && !request.headers.get(X_FORWARDED_PROTO).getOrElse("").contains("https")) {
      Some(controllers.Application.redirect)
    } else {
      super.onRouteRequest(request)
    }
  }

}
