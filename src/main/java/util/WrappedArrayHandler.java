package util;

import java.util.ArrayList;

public  class WrappedArrayHandler {
	
	public static ArrayList<String> unWrap(String s)
	{
		ArrayList res = new ArrayList<String>();
		s= s.substring(s.indexOf("(")+1, s.lastIndexOf(")")); //remove [WrappedArray(...)]
		if (s==null || s.isEmpty() || s.equalsIgnoreCase("null"))
			return res; 
		String [] elements = s.split(", ");
		for (String e : elements){   //e is a pair of the modulename,librarypath
			/*
			String [] parts= e.split(",");
				for (String p:parts)
					res.add(p);
					*/
			res.add(e);
		}
		
		return res;
	}
}
