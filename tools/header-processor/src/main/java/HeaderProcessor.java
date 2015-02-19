import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;

public class HeaderProcessor {

    private String resourcePath = "src/main/resources/";
    private String configFilename = resourcePath + "headerProcessor.config";
    private String currentHeaderKey = "currentHeaderFilename";
    private String newHeaderKey = "newHeaderFilename";
    private String fileExtensionKey = "fileExtension";
    private String rootKey = "root";
    private String logKey = "log";
    private String fieldSeparator = "=";
    private String valueSeparator = ",";
    private String newLine = "\n";

    private File logFile = null;

    public static void main(String[] args) {

        HeaderProcessor headerProcessor = new HeaderProcessor();
        headerProcessor.processFiles();
    }

    Dictionary<String, String> getConfigValues(String configFilename) {
        Dictionary<String, String> configValues = new Hashtable<String, String>();

        File configFile = new File(configFilename);
        if (!configFile.exists()) {
            return null;
        }
        try {
            String line;
            BufferedReader reader = new BufferedReader(new FileReader(configFilename));
            while ((line = reader.readLine()) != null) {
                String[] keyValue = line.split(fieldSeparator);
                if (keyValue.length != 2) {
                    return null;
                }
                configValues.put(keyValue[0].trim(), keyValue[1].trim());
            }
        } catch (Exception e) {
            //
            // swallow e for now
            //
            return null;
        }
        return configValues;
    }

    void processFiles() {
        Dictionary<String, String> configValues = getConfigValues(configFilename);
        if (configValues != null) {
            String[] currentHeaders = configValues.get(currentHeaderKey).split(valueSeparator);
            File[] currentHeaderFiles = new File[currentHeaders.length];
            for (int i = 0; i < currentHeaders.length; i++) {
                currentHeaderFiles[i] = new File(resourcePath + currentHeaders[i].trim());
            }
            File newHeader = new File(resourcePath + configValues.get(newHeaderKey));
            File rootDirectory = new File(configValues.get(rootKey));
            try {
                logFile = new File(configValues.get(logKey));
            }
            catch (Exception e)
            {
                //
                // swallow, nothing to do
                //
            }

            boolean templatesAvailable = true;
            for (File file : currentHeaderFiles)
            {
                templatesAvailable = templatesAvailable && file.exists();
            }
            templatesAvailable = templatesAvailable && newHeader.exists();
            templatesAvailable = templatesAvailable && rootDirectory.exists();

            if (templatesAvailable) {
                List<File> files = getFiles(configValues.get(rootKey), configValues.get(fileExtensionKey));
                for (File file : files) {
                    addOrReplaceHeader(file.getAbsolutePath(), currentHeaderFiles, newHeader);
                }
            }
        }
    }

    void addOrReplaceHeader(String sourceFilename, File[] currentHeaders, File newHeader)
    {
        try {
            String scalaBuffer = FileUtils.readFileToString(new File(sourceFilename));
            String newHeaderBuffer = FileUtils.readFileToString(newHeader);
            String replaceWithBuffer = newHeaderBuffer + newLine;
            File sourceFile = new File(sourceFilename);

            for (int i = 0; i < currentHeaders.length; i++) {
                String currentHeader = FileUtils.readFileToString(currentHeaders[i]);

                if (StringUtils.contains(scalaBuffer, currentHeader)) {
                    scalaBuffer = replaceWithBuffer + scalaBuffer.replace(currentHeader, "").trim() + newLine;
                    FileUtils.write(sourceFile, scalaBuffer);
                    return;
                }
            }
            if (!StringUtils.contains(scalaBuffer, newHeaderBuffer)) {
                scalaBuffer = replaceWithBuffer + scalaBuffer.trim() + newLine;
                FileUtils.write(sourceFile, scalaBuffer);
            }
            else {
                Log("Update not needed: " + sourceFilename + newLine);
            }
        }
        catch (Exception e)
        {
            Log("Error processing: " + sourceFilename + newLine);
        }
    }

    void Log(String info)
    {
        try {
            FileUtils.write(logFile, info , true);
        }
        catch (Exception e)
        {
            //
            // nothing to do
            //
        }
    }
    List<File> getFiles(String directoryFilename, String fileExtension) {
        File currentDirectory = new File(directoryFilename);
        List<File> fileList = new ArrayList<File>();

        File[] currentDirectoryFiles = currentDirectory.listFiles();
        for (File file : currentDirectoryFiles) {
            if (file.isFile() && file.getName().endsWith(fileExtension)) {
                fileList.add(file);
            } else {
                if (file.isDirectory()) {
                    fileList.addAll(getFiles(file.getAbsolutePath(), fileExtension));
                }
            }
        }
        return fileList;
    }
}