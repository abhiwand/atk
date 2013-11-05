import com.typesafe.config.ConfigFactory
import play.api.{Mode, Configuration, GlobalSettings}
import java.io.File

object Global extends GlobalSettings{
  override def onLoadConfig(config: Configuration, path: File, classloader: ClassLoader, mode: Mode.Mode): Configuration = {
    val modeSpecificConfig = config ++ Configuration(ConfigFactory.load(s"application.${mode.toString.toLowerCase}.conf"))
    super.onLoadConfig(modeSpecificConfig, path, classloader, mode)
  }

}
