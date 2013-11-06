import com.typesafe.config.ConfigFactory
import play.api.{Mode, Configuration, GlobalSettings}
import java.io.File
import scala.collection.JavaConversions._

object Global extends GlobalSettings {
    override def onLoadConfig(config: Configuration, path: File, classloader: ClassLoader, mode: Mode.Mode): Configuration = {
        //if we get a play mode environment variable use that instead of the default one from play
        val playMode = if (System.getProperty("play.config") == null) mode.toString.toLowerCase else System.getProperty("play.config").toLowerCase
        //load our config
        val loadConfig = ConfigFactory.load(s"application.${playMode}.conf")

        val customConfigs = loadConfig.withOnlyPath(playMode.toLowerCase).entrySet map (_.getKey)
        for (customConfig <- customConfigs) {
            System.setProperty(customConfig.replace(playMode + ".", ""), loadConfig.getString(customConfig))
        }

        var modeSpecificConfig = Configuration(loadConfig)

        if (mode != Mode.Test)
            modeSpecificConfig = config ++ modeSpecificConfig

        if (!playMode.eq(Mode.Prod.toString.toLowerCase)) {
            System.out.println("play mode: " + playMode)
        }

        //pass the regular play mode since our own custom mode is only use for loading a config
        super.onLoadConfig(modeSpecificConfig, path, classloader, mode)
    }
}
