//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

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
    private String currentHeaderPythonKey = "currentHeaderFilenamePython";
    private String newHeaderPythonKey = "newHeaderFilenamePython";
    private String fileExtensionKey = "fileExtension";
    private String pythonFileExtension = ".py";
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

    File[] getFileHandles (String[] filenames)
    {
        File[] filesHandles = new File[filenames.length];
        for (int i = 0; i < filesHandles.length; i++) {
            filesHandles[i] = new File(resourcePath + filenames[i].trim());
        }
        return filesHandles;
    }

    boolean templatesAvailable (File[]currentHeaders, File[]currentHeadersPython, File newHeader, File newHeaderPython, File rootDirectory)
    {
        boolean templatesAvailable = true;
        for (File file : currentHeaders)
        {
            templatesAvailable = templatesAvailable && file.exists();
        }
        for (File file : currentHeadersPython)
        {
            templatesAvailable = templatesAvailable && file.exists();
        }
        templatesAvailable = templatesAvailable && newHeader.exists();
        templatesAvailable = templatesAvailable && newHeaderPython.exists();
        templatesAvailable = templatesAvailable && rootDirectory.exists();

        return templatesAvailable;
    }

    void processFiles() {
        Dictionary<String, String> configValues = getConfigValues(configFilename);
        if (configValues != null) {
            File[] currentHeaders = getFileHandles(configValues.get(currentHeaderKey).split(valueSeparator));
            File[] currentHeadersPython = getFileHandles(configValues.get(currentHeaderPythonKey).split(valueSeparator));

            File newHeader = new File(resourcePath + configValues.get(newHeaderKey));
            File newHeaderPython = new File(resourcePath + configValues.get(newHeaderPythonKey));

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

            boolean templatesAvailable = templatesAvailable (currentHeaders,
                                                             currentHeadersPython,
                                                             newHeader,
                                                             newHeaderPython,
                                                             rootDirectory);
            if (templatesAvailable) {
                String[] fileExtensions = configValues.get(fileExtensionKey).split(valueSeparator);
                if (fileExtensions != null && fileExtensions.length > 0) {
                    File newHeaders;
                    File[] oldHeaders;
                    for (String extension : fileExtensions) {
                        List<File> files = getFiles(configValues.get(rootKey), extension.trim());
                        if (!extension.endsWith(pythonFileExtension)) {
                            oldHeaders = currentHeaders;
                            newHeaders = newHeader;
                        }else
                        {
                            oldHeaders = currentHeadersPython;
                            newHeaders = newHeaderPython;
                        }
                        for (File file : files) {
                            addOrReplaceHeader(file.getAbsolutePath(), oldHeaders, newHeaders);
                        }
                    }
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
