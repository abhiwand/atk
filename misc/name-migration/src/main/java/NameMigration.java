/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;

public class NameMigration {

    private String resourcePath = "src/main/resources/";
    private String configFilename = resourcePath + "name-migration.config";
    private String textPatterns = "textPatterns";
    private String fileExtensions = "fileExtensions";
    private String fileAndDirectoryPatterns = "fileAndDirectoryPatterns";
    private String filenames = "filenames";
    private String root = "root";
    private String log = "log";
    private String fieldSeparator = "=";
    private String valueSeparator = ",";
    private String patternSeparator = "!";
    private String newLine = "\n";

    private File logFile = null;

    public static void main(String[] args) {

        NameMigration sanitizer = new NameMigration();
        sanitizer.processFiles();
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
            String textPatterns = configValues.get(this.textPatterns);
            String[] patternTuples = textPatterns.split(patternSeparator);
            File rootDirectory = new File(configValues.get(root));
            try {
                logFile = new File(configValues.get(log));
            }
            catch (Exception e)
            {
                //
                // swallow, nothing to do
                //
            }

            if (rootDirectory.exists() && (null != patternTuples) && (patternTuples.length > 0)) {

                //file and directory names
                String fileAndDirectoryPatterns = configValues.get(this.fileAndDirectoryPatterns);
                sanitizeFilesAndDirs(configValues.get(root), fileAndDirectoryPatterns.split(patternSeparator));

                //file content
                sanitizeFileContent(filenames, configValues, patternTuples);
                sanitizeFileContent(fileExtensions, configValues, patternTuples);
            }
        }
    }

    void sanitizeFilesAndDirs(String directoryFilename, String[] fileAndDirPatterns) {

        File currentDirectory = new File(directoryFilename);
        File[] currentDirectoryFiles = currentDirectory.listFiles();
        for (File file : currentDirectoryFiles) {
            String fullName = file.getAbsolutePath();
            if (file.isDirectory()) {
                sanitizeFilesAndDirs(fullName, fileAndDirPatterns);
            }

            renameFileOrDir(fullName.toLowerCase(), file, fileAndDirPatterns);
        }
    }

    void renameFileOrDir(String currentName, File currentFileOrDir, String[] fileAndDirPatterns) {
        for (int i = 0; i < fileAndDirPatterns.length; i++) {
            String[] pairs = fileAndDirPatterns[i].split(valueSeparator);
            if (pairs.length > 1) {
                String newName = currentName.replace(pairs[0].trim(), pairs[1].trim());
                if (currentName.equals(newName) == false) {
                    currentFileOrDir.renameTo(new File(newName));
                    return;
                }
            }
        }
    }

    void sanitizeFileContent(String configKey, Dictionary<String, String> configValues, String[] patterns) {
        String[] names = configValues.get(configKey).split(valueSeparator);
        if (names != null && names.length > 0) {
            for (String name : names) {
                List<File> files = getFiles(configValues.get(root), name.trim());
                for (File file : files) {
                    applyPatterns(file.getAbsolutePath(), patterns);
                }
            }
        }
    }

    void applyPatterns(String sourceFilename, String[] patternTuples)
    {
        try {
            String fileBuffer = FileUtils.readFileToString(new File(sourceFilename));
            File destFile = new File(sourceFilename);
            String result = fileBuffer;

            for (int i = 0; i < patternTuples.length; i++) {
                String[] pattern = patternTuples[i].split(valueSeparator);
                String replaceWith = pattern.length > 1 ? pattern[1].trim() : StringUtils.EMPTY;
                result = result.replace(pattern[0].trim(), replaceWith);
            }
            FileUtils.write(destFile, result);
        }
        catch (Exception e)
        {
            log("Error processing: " + sourceFilename + newLine);
        }
    }

    void log(String info)
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

    List<File> getFiles(String directoryFilename, String fileOrExtension) {
        File currentDirectory = new File(directoryFilename);
        List<File> fileList = new ArrayList<File>();

        File[] currentDirectoryFiles = currentDirectory.listFiles();
        for (File file : currentDirectoryFiles) {
            if (file.isFile() && file.getName().endsWith(fileOrExtension)) {
                fileList.add(file);
            } else {
                if (file.isDirectory()) {
                    fileList.addAll(getFiles(file.getAbsolutePath(), fileOrExtension));
                }
            }
        }
        return fileList;
    }
}
