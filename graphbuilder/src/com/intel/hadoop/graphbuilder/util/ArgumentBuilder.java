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
     * @return ArgumentBuilder Instance
     */
    public static ArgumentBuilder newArguments(){
        return new ArgumentBuilder();
    }

    /**
     * add an argument
     *
     * @param name variable name
     * @param object variable to set
     * @return updated ArgumentBuilder instance
     */
    public ArgumentBuilder with(String name, Object object){

        this.arguments.put(name, object);

        return this;
    }

    /**
     * remove an argument
     *
     * @param name the variables name
     * @return updated ArgumentBuilder instance
     */
    public ArgumentBuilder withOut(String name){

        this.arguments.remove(name);

        return this;
    }

    /**
     * get the stored object
     *
     * @param name the name of the variable to return
     * @return the requested argument
     */
    public Object get(String name){

        return arguments.get(name);
    }

    /**
     * get an argument if it exists else return the default value
     *
     * @param name the arguments name
     * @param defaultValue desired default value if the argument doesn't exists
     * @return requested argument
     */
    public Object get(String name, Object defaultValue){
        if(this.exists(name)){
            return this.get(name);
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
     * @param name the argument to check
     * @param klass the class to verify against
     * @return status of the type check
     */
    public boolean isType(String name, Class klass){
        if(this.exists(name) && this.get(name).getClass().equals(klass)){

            return true;
        }else{
            return false;
        }
    }

    /**
     * does the argument exists
     *
     * @param name argument name to check for
     * @return status of an argument
     */
    public boolean exists(String name){

        return arguments.containsKey(name);
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
