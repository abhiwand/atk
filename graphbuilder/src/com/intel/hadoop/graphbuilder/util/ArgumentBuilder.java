package com.intel.hadoop.graphbuilder.util;


import java.util.HashMap;

public class ArgumentBuilder {
    private HashMap<String, Object> arguments;


    ArgumentBuilder(){
        this.arguments = new HashMap<>();
    }

    public static ArgumentBuilder newArguments(){
        return new ArgumentBuilder();
    }

    public ArgumentBuilder with(String name, Object object){

        this.arguments.put(name, object);

        return this;
    }

    /*public ArgumentBuilder withOUt(String name){

        this.arguments.remove(name);

        return this;
    }*/

    public ArgumentBuilder withOut(String name){

        this.arguments.remove(name);

        return this;
    }

    public Object get(String name){

        return arguments.get(name);
    }

    public boolean isEmpty(){

        return this.arguments.isEmpty();
    }

    public boolean isType(String name, Class klass){
        if(this.exists(name) && this.get(name).getClass().equals(klass)){

            return true;
        }else{
            return false;
        }
    }

    public boolean exists(String name){

        return arguments.containsKey(name);
    }

    public boolean exists(Object object){

        return arguments.containsValue(object);
    }


}
