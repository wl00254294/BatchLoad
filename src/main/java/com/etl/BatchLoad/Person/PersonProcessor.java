package com.etl.BatchLoad.Person;
import org.springframework.batch.item.ItemProcessor;
public class PersonProcessor implements ItemProcessor<Person, Person> {
	  @Override
	  public Person process(Person person) throws Exception {
		 Person outData = new Person();
		 outData.setFirstName(person.getFirstName()+"Out");
		 outData.setLastName(person.getLastName()+"Out");
		 
		  
	    String greeting =  person.getFirstName() + " "
	        + person.getLastName() + "!";

	   System.out.println(greeting);
	    return outData;
	  }
}
