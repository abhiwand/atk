/**
 * Copyright (C) 2012 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.util;


import java.util.HashMap;

/**
 * Argument Builder makes it a litter nicer to deal with variable length argument params. instead of doing Type ... name
 * and getting an array you would do ArgumentBuilder.newArguments().with("Var name", var) or
 * new ArgumentBuilder().with("var name", var). To get a variable do ArgumentBuilder.get("var name").
 * it keeps you from having to remembering the exact param order in the variable length arrays. It's a wrapper on a
 * String, Object hash map
 *
 */
public class ArgumentBuilder {
    private HashMap<String, Object> arguments;

    ArgumentBuilder(){
        this.arguments = new HashMap<>();
    }

    /**
     * a static constructor to save folks from having to do a new
     *
     * @return ArgumentBuilder instance
     */
    public static ArgumentBuilder newArguments(){
        return new ArgumentBuilder();
    }

    /**
     * add an argument
     *
     * @param argumentName variable name
     * @param object variable to set
     * @return updated ArgumentBuilder instance
     */
    public ArgumentBuilder with(String argumentName, Object object){

        this.arguments.put(argumentName, object);

        return this;
    }

    /**
     * remove an argument
     *
     * @param argumentName the variables name
     * @return updated ArgumentBuilder instance
     */
    public ArgumentBuilder withOut(String argumentName){

        this.arguments.remove(argumentName);

        return this;
    }

    /**
     * get the stored object
     *
     * @param argumentName the name of the variable to return
     * @return the requested argument
     */
    public Object get(String argumentName){

        return arguments.get(argumentName);
    }

    /**
     * get an argument if it exists else return the default value
     *
     * @param argumentName the arguments name
     * @param defaultValue desired default value if the argument doesn't exists
     * @return requested argument
     */
    public Object get(String argumentName, Object defaultValue){
        if(this.exists(argumentName)){
            return this.get(argumentName);
        }else{
            return defaultValue;
        }
    }


    /**
     * see if the argument list empty
     *
     * @return empty argument list status
     */
    public boolean isEmpty(){

        return this.arguments.isEmpty();
    }

    /**
     * check a stored arguments type vs the given type
     *
     * @param argumentName the argument to check
     * @param klass the class to verify against
     * @return status of the type check
     */
    public boolean isType(String argumentName, Class klass){
        if(this.exists(argumentName) && this.get(argumentName).getClass().equals(klass)){

            return true;
        }else{

            return false;
        }
    }

    /**
     * does the argument exists
     *
     * @param argumentName argument name to check for
     * @return status of an argument
     */
    public boolean exists(String argumentName){

        return arguments.containsKey(argumentName);
    }

    /**
     * does the argument exists
     *
     * @param object object to check for
     * @return status of an argument
     */
    public boolean exists(Object object){

        return arguments.containsValue(object);
    }


}
