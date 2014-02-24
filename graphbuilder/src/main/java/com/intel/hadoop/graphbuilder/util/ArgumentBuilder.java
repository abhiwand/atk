/**
 * Copyright (C) 2013 Intel Corporation.
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
 * Argument Builder makes it a little nicer to deal with variable length argument 
 * params. Instead of using <code>Type ... name</code> and getting an array, you would use
 * <code>ArgumentBuilder.newArguments().with("Var name", var)</code> or
 * <code>new ArgumentBuilder().with("var name", var)</code>. To get a variable use
 * <code>ArgumentBuilder.get("var name")</code>. This way, you do not have to remember
 * the exact param order in the variable length arrays. It's a wrapper on a
 * String, Object hash map.
 *
 */
public class ArgumentBuilder {
    private HashMap<String, Object> arguments;

    ArgumentBuilder(){
        this.arguments = new HashMap<String, Object>();
    }

    /**
     * A static constructor to save folks from having to do a new
     *
     * @return ArgumentBuilder instance
     */
    public static ArgumentBuilder newArguments(){
        return new ArgumentBuilder();
    }

    /**
     * Adds an argument.
     *
     * @param argumentName The variable name.
     * @param object The variable to set.
     * @return  An updated ArgumentBuilder instance.
     */
    public ArgumentBuilder with(String argumentName, Object object){

        this.arguments.put(argumentName, object);

        return this;
    }

    /**
     * Removes an argument.
     *
     * @param argumentName The name of the variable.
     * @return An updated ArgumentBuilder instance.
     */
    public ArgumentBuilder withOut(String argumentName){

        this.arguments.remove(argumentName);

        return this;
    }

    /**
     * Gets the stored object.
     *
     * @param argumentName The name of the variable to return.
     * @return The requested argument.
     */
    public Object get(String argumentName){

        return arguments.get(argumentName);
    }

    /**
     * Gets an argument if it exists, otherwise returns the default value.
     *
     * @param argumentName  The name of the argument.
     * @param defaultValue  The desired default value if the argument doesn't exist.
     * @return The requested argument.
     */
    public Object get(String argumentName, Object defaultValue){
        if(this.exists(argumentName)){
            return this.get(argumentName);
        }else{
            return defaultValue;
        }
    }


    /**
     * See if the argument list empty.
     *
     * @return The empty argument list status.
     */
    public boolean isEmpty(){

        return this.arguments.isEmpty();
    }

    /**
     * Checks the type of a stored argument type against the given type.
     *
     * @param argumentName The argument to check.
     * @param klass The class to verify against.
     * @return The status of the type check.
     */
    public boolean isType(String argumentName, Class klass){
        return this.exists(argumentName) && this.get(argumentName).getClass().equals(klass);
    }

    /**
     * Checks if the argument exists.
     *
     * @param argumentName  The argument name for which to check.
     * @return The status of an argument.
     */
    public boolean exists(String argumentName){

        return arguments.containsKey(argumentName);
    }

    /**
     * Checks if the argument exists.
     *
     * @param object  The object for which to check.
     * @return The status of the argument.
     */
    public boolean exists(Object object){

        return arguments.containsValue(object);
    }


}
