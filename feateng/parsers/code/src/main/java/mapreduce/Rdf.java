package  mapreduce;

import org.apache.hadoop.io.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Rdf {
	private Text subject;
	private Text predicate;
	private Text object;
	private boolean isSet = false;

	public Rdf(){
		set(new Text(),new Text(), new Text());
	}
	
	public Rdf(Text subject, Text predicate, Text object){
		set(subject,predicate,object);
	}
	
	
	
	public Rdf(String subject, String predicate,String object){
		set(new Text(subject), new Text(predicate), new Text(object));
	}
	
	private void set(Text subject, Text predicate, Text object){
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		
	}
	
	public void setNTriples(String text) throws Exception{
		String regexPattern = "^<([^>]+)>\\s+<([^>]+)>\\s+<?([^>]+)>?\\s+\\.$";
		Pattern pattern = Pattern.compile(regexPattern);
    	Matcher matcher = pattern.matcher(text);
    	if (matcher.matches()) {
    		setSubject(new Text(matcher.group(1)));
    		setPredicate(new Text(matcher.group(2)));
    		setObject(new Text(matcher.group(3)));
    		this.isSet = true;
    	}
    	else{
            //throw exception
            throw new Exception("bad formatted N-triples");
        }
    
    	
	}
	
	public boolean isSet(){return this.isSet;}
	
	public void setSubject(Text subject){this.subject = subject;}
	public void setPredicate(Text predicate){this.predicate = predicate;}
	public void setObject(Text object){this.object = object;}
	
	public Text getSubject(){return this.subject;}
	public Text getPredicate(){return this.predicate;}
	public Text getObject(){return this.object;}
	

	
	
	

}
