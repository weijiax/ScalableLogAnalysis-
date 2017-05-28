package util;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

public class ModelTransformation {
 
	//fixed header? 
	/*
	<?xml version="1.0"?>
	<PMML version="4.1" xmlns="http://www.dmg.org/PMML-4_1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.dmg.org/PMML-4_1 http://www.dmg.org/v4-1/pmml-4-1.xsd">
	 <Header copyright="Copyright (c) 2015 xwj" description="arules association rules model">
	  <Extension name="user" value="xwj" extender="Rattle/PMML"/>
	  <Application name="Rattle/PMML" version="1.4"/>
	  <Timestamp>2015-10-15 17:21:54</Timestamp>
	 </Header>
	 <DataDictionary numberOfFields="2">
	  <DataField name="transaction" optype="categorical" dataType="string"/>
	  <DataField name="item" optype="categorical" dataType="string"/>
	 </DataDictionary>
	 */
	public static String pmmlHeader()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append("<?xml version=\"1.0\"?>"
				+ "<PMML version=\"4.1\" xmlns=\"http://www.dmg.org/PMML-4_1\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
				+ " xsi:schemaLocation=\"http://www.dmg.org/PMML-4_1 http://www.dmg.org/v4-1/pmml-4-1.xsd\">"
				+ "<Header copyright=\"Copyright (c) 2015 xwj\" description=\"FPGrowth MLlib Assoicaiton rules model\">"
				+ "<Extension name=\"user\" value=\"xwj\" extender=\"Rattle/PMML\"/>"
				+ "<Application name=\"Rattle/PMML\" version=\"1.4\"/>"
				+ "<Timestamp>"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"</Timestamp>"
				+ "</Header>"
				+ "<DataDictionary numberOfFields=\"2\">"
				+ "<DataField name=\"transaction\" optype=\"categorical\" dataType=\"string\"/>"
				+ "<DataField name=\"item\" optype=\"categorical\" dataType=\"string\"/>"
				+ "</DataDictionary>"
		);
		return buffer.toString();
	}
	
	public static void writeStringtoFile(String path, String context)
	{
		 try{
			 File file = new File(path);
			 if (!file.exists()) {
					file.createNewFile();
				}
			 FileWriter fw = new FileWriter(file.getAbsoluteFile());
			 fw.write(context);
			 fw.flush();
			 fw.close();
		 }catch(Exception e)
		 {
			 System.out.println(e.toString());
		 }
	}
	
	public static String toPMML(FPGrowthModel<String> fpg, long transactions_num, double min_sup, double minConfidence)
	{
		StringBuffer buffer = new StringBuffer();
		
		List<FPGrowth.FreqItemset<String>> itemsets= fpg.freqItemsets().toJavaRDD().collect();
		List<AssociationRules.Rule<String>> rules = fpg.generateAssociationRules(minConfidence).toJavaRDD().collect();
		
		System.out.print(rules);
	
		//store the frequency of each itemset for LIFT and support calculation. 
		HashMap<List<String>, Double> itemsets_freq = new HashMap<List<String>, Double>();
		for (FPGrowth.FreqItemset<String> itemset : itemsets)
			itemsets_freq.put(itemset.javaItems(), (double) itemset.freq()/transactions_num);
		
		//Create Association Rule Strings
		StringBuffer ruleBuffer= new StringBuffer();
		HashMap<List<String>, Integer> itemsets_hm= new  HashMap<List<String>, Integer>();
		int idx=1;
		for (AssociationRules.Rule<String> rule: rules)
		{
			List<String> lh= rule.javaAntecedent();
			List<String> rh= rule.javaConsequent();
			if (!itemsets_hm.containsKey(lh))
				itemsets_hm.put(lh, idx++);
			if (!itemsets_hm.containsKey(rh))
				itemsets_hm.put(rh, idx++);
			//note: there is no support, lift value avaliable from rule. used confidnce instead. 
			//supp(X U Y )= conf( X=>Y) * supp(X);
			//lift(X U Y) =  supp(X U Y) / (SUP(X)* SUP(Y)) = Conf(X=Y) /sup(Y);

			ruleBuffer.append("<AssociationRule support=\"" + rule.confidence() * itemsets_freq.get(lh) 
					+ "\" confidence=\""+ rule.confidence() +"\" lift=\"" + rule.confidence() / itemsets_freq.get(rh)
					+"\" antecedent=\"" +itemsets_hm.get(lh)+ "\" consequent=\"" + itemsets_hm.get(rh)+ "\"/>" );
		}
		//Now create the itemset strings;
		StringBuffer itemsetBuffer = new StringBuffer();
		HashMap <String, Integer> items= new HashMap<String, Integer>();
		idx=1;
		for (List<String> itemList : itemsets_hm.keySet())
		{
			itemsetBuffer.append("<Itemset id=\"" + itemsets_hm.get(itemList)+"\" numberOfItems=\""+ itemList.size()+"\">");
			
			for (String s : itemList)
			{
				if (!items.containsKey(s))
					items.put(s, idx++);
				itemsetBuffer.append("<ItemRef itemRef=\""+items.get(s)+"\"/>");
			}
			itemsetBuffer.append("</Itemset>");
		}
		
		/*
		for (FPGrowth.FreqItemset<String> itemset : itemsets)
			for (String s : itemset.javaItems())
				if (!items.containsKey(s))
					items.put(s, idx++);
	*/
		
		//now ready to create the final string. 
		buffer.append(pmmlHeader());
		buffer.append("\n");
		
		
		//Model statistics 
		/*
		 <AssociationModel functionName="associationRules" numberOfTransactions="9835" numberOfItems="169" minimumSupport="0.001" minimumConfidence="0.5" numberOfItemsets="16" numberOfRules="10">
		  <MiningSchema>
		   <MiningField name="transaction" usageType="group"/>
		   <MiningField name="item" usageType="active"/>
		  </MiningSchema>
		  */		
		buffer.append("<AssociationModel functionName=\"associationRules\"" 
				+ " numberOfTransactions=\""+transactions_num +"\""
				+ " minimumSupport=\""+min_sup+"\""
				+ " minimumConfidence=\""+minConfidence+"\""
				+ " numberOfItemsets=\"" +itemsets.size()+"\""
				+ " numberOfRules=\""+ rules.size()+"\">"
		);
		buffer.append("\n");
		buffer.append("<MiningSchema> <MiningField name=\"transaction\" usageType=\"group\"/> "
				+ " <MiningField name=\"item\" usageType=\"active\"/>"
				+ " </MiningSchema>");
		
				
		//now append items
		for (String s : items.keySet())	
			buffer.append("<Item id=\""+ items.get(s)+ "\" value=\""+s+"\"/>");
		//now append itemsets
		buffer.append("\n");
		buffer.append(itemsetBuffer.toString());
		
		//now append rules
		buffer.append("\n");
		buffer.append(ruleBuffer.toString());

		buffer.append("</AssociationModel> </PMML>");
		
		//System.out.println(buffer.toString());
		return buffer.toString();
	}
}
