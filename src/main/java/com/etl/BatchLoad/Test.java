package com.etl.BatchLoad;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jdom2.*;
import org.jdom2.input.SAXBuilder;

import com.etl.BatchLoad.comm.CacheInfoDAO;

public  class Test {

	public static List<Element> book_children;
	public static void main(String[] args) {
        
		int[] i = {1,2};
		int k = i[2];
		//System.out.println(i);
		

	}
	public static List<CacheInfoDAO> JDOM(String jobname) {
		 List<CacheInfoDAO> out = new ArrayList<CacheInfoDAO>();
		 SAXBuilder builder=new SAXBuilder();
         try {
			Document document=builder.build(new FileInputStream("C:\\Users\\wtakung\\git\\BatchLoad\\src\\main\\resources\\cache_table.xml"));
			Element book_root=document.getRootElement();
		
			//獲去所有的書籍
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
