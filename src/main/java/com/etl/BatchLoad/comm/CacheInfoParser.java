package com.etl.BatchLoad.comm;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.springframework.core.io.ClassPathResource;

public class CacheInfoParser {
	

	public static List<CacheInfoDAO> getRefInfo(String jobname) {
		 List<CacheInfoDAO> out = new ArrayList<CacheInfoDAO>();
		 SAXBuilder builder=new SAXBuilder();
         try {        	
        	 Document document=builder.build(new ClassPathResource("cache_table.xml").getInputStream());
        	 
        	 Element book_root=document.getRootElement();
			 List<Element> book_children = null;
		
			List<Element> book_list=book_root.getChildren();
			for (Element book : book_list) {
				
				
				List<Attribute> book_attr=book.getAttributes();
				 for (Attribute attr : book_attr) {
						//System.out.println(attr.getName()+":"+attr.getValue());
						if(attr.getName().equals("name") && jobname.equals(attr.getValue())){
							book_children=book.getChildren();
							break;
						}
				}
				
				
			
				 for (Element element : book_children) {
					 List<Attribute> book_attr2=element.getAttributes();
					 for (Attribute attr : book_attr2) {
							//System.out.println(attr.getName()+":"+attr.getValue());
							CacheInfoDAO item=new CacheInfoDAO();
							item.setCacheName(attr.getValue());
							
							List<Element> child=element.getChildren();
							//System.out.println(child.size());
							for(Element ele : child)
							{
								if(ele.getName().equals("sql"))
								{
									item.setSql(ele.getValue());
								}
								if(ele.getName().equals("keycolumn"))
								{
									item.setKeycolumn(ele.getValue());
								}								
								if(ele.getName().equals("valuecolumn"))
								{
									item.setValuecolumn(ele.getValue());
								}
							}
							
							out.add(item);

					}
				}

				 
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
         
         return out;
  }
}
