package com.demo.reading;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import com.demo.reading.parser.XmlDomParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
 
public class MeterReadingMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private MultipleOutputs mos;
	private final String SEPERATOR=",";
	private final String HYPHEN="-";
	private final String  NEXT_LINE = "\n";
	private final String METER_READINGS = "MeterReadings";
	private final String[] mainNodes = {"ConsumptionData", "MaxDemandData", "CumulativeDemandData", "CoincidentDemandData", "PresentDemandData","IntervalData"};
	private List mainNodesData = Arrays.asList(mainNodes);

    @Override
    protected void setup(Context context){
    	mos = new MultipleOutputs(context);
	}
 
  
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	System.out.println("content : "+value);

        try {
			Map<String, StringBuilder> parentMap = new LinkedHashMap<>();
			Map<String, String> map = new LinkedHashMap<>();
			String row = "";
			List<String> rows = new ArrayList<>();
			createDefaultMap(map);
			createDefaultParentMap(parentMap);

            InputStream is = new ByteArrayInputStream(value.toString().getBytes());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(is);

            doc.getDocumentElement().normalize();

            NodeList nList = doc.getElementsByTagName(METER_READINGS);

			for (int i = 0; i < nList.getLength(); i++) {
				Node nNodeRe = nList.item(i);
				if (nNodeRe.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNodeRe;
					NodeList meterList = doc.getElementsByTagName("Meter");
					Node meterNode = meterList.item(i);
					List<Node> nodes = XmlDomParser.extractAllChildren(eElement, mainNodesData);
					for (Node node : nodes){
						if (meterNode.getNodeType() == Node.ELEMENT_NODE){
							map.put("METERNAME", XmlDomParser.getAttributeByName(meterNode, "MeterName"));
						}
						if(node.getNodeName().equals("ConsumptionData")){
							rows = getConsumptionData(node, map);
						}else if (node.getNodeName().equals("MaxDemandData")){
							rows = getMaxDemandData(node, map);
						}else if(node.getNodeName().equals("CumulativeDemandData")){
							rows = getCumulativeDemandData(node, map);
						}else if(node.getNodeName().equals("CoincidentDemandData")){
							rows = getCoincidentDemandData(node, map);
						}else if(node.getNodeName().equals("PresentDemandData")){
							rows = getPresentDemandData(node, map);
						}else if (node.getNodeName().equals("IntervalData")){
							rows = getInterValData(node, map, context);
						}
						writeDataToMap(parentMap, rows, node.getNodeName());
						createDefaultMap(map);
					}
				}
			}
			for (Map.Entry<String, StringBuilder> entry : parentMap.entrySet()) {
				if (entry.getValue().length() > 0) {
					writeCSVDataToOutFile(entry.getKey(), entry.getValue(), context);
				}
			}

        } catch (Exception e) {
            System.out.println("Error in parsing the xml data");
			e.printStackTrace();
        }

    }

	public void createDefaultMap(Map<String, String> map){
		map.put("METERNAME", "");
		map.put("TYPE", "");
		map.put("UOM", "");
		map.put("READINGTYPE", "");
		map.put("DIRECTION", "");
		map.put("TOUBUCKET","");
		map.put("MEASUREMENTPERIOD","");
		map.put("MULTIPLIER","");
		map.put("TIMESTAMP","");
		map.put("ESTTIME","");
		map.put("VALUE","");
		map.put("INTERVAL","");

		createDefaultIntervalData(map);


		System.out.println("createDefaultMap "+ map);
	}

	public static void createDefaultIntervalData(Map<String, String> map){
		map.put("TIMECHANGED", "");
		map.put("CLOCKSETBACKWARD", "");
		map.put("LONGINTERVAL", "");
		map.put("CLOCKSETFORWARD", "");
		map.put("PARTIALINTERVAL", "");
		map.put("INVALIDTIME", "");
		map.put("SKIPPEDINTERVAL","");
		map.put("COMPLETEOUTAGE", "");
		map.put("PULSEOVERFLOW","");
		map.put("TESTMODE", "");
		map.put("TAMPER", "");
		map.put("PARTIALOUTAGE", "");
		map.put("SUSPECTEDOUTAGE", "");
		map.put("RESTORATION","");
		map.put("DST", "");
		map.put("INVALIDVALUE","");
	}


	public void createDefaultParentMap(Map<String, StringBuilder> parentMap){
		parentMap.put("ConsumptionData", new StringBuilder());
		parentMap.put("MaxDemandData", new StringBuilder());
		parentMap.put("CumulativeDemandData", new StringBuilder());
		parentMap.put("CoincidentDemandData", new StringBuilder());
		parentMap.put("PresentDemandData", new StringBuilder());
		parentMap.put("IntervalData", new StringBuilder());

		System.out.println("createDefaultParentMap "+ parentMap);

	}

	public List<String> getInterValData(Node node, Map<String, String> map, Context context){
		String[] requiredEle = {"Reading"};
		List<String> list = Arrays.asList(requiredEle);
		Node firstChild1Node = XmlDomParser.getFirstChild((Element)node, "IntervalSpec");
		Element firstChildElement = (Element) firstChild1Node;
		XmlDomParser.getAllAttributes(firstChildElement, map);

		String readingType = getReadingType(map, node.getNodeName());
		map.put("READINGTYPE", readingType);

		ArrayList<String> rows = new ArrayList<>();

		List<Node> nodes = XmlDomParser.extractAllChildren((Element)node, list);
		for (Node childNode : nodes){
			XmlDomParser.visitRecursively(childNode, map);
			//System.out.println("printing map recursively "+ map);
			map.put("ESTTIME", map.get("TIMESTAMP"));
			String row = createCSVRowWithAppnderForIntervalData(map);
			System.out.println("pring row in recursion "+row);
			rows.add(row);
			createDefaultIntervalData(map);
		}

		System.out.println("Al Size is "+rows.size());
		try {
			context.write(new Text("IntervalData"), new Text(("ArrayList size is "+rows.size())));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return rows;

	}


	private List<String> getCoincidentDemandData(Node node, Map<String, String> map) {
		String[] requiredEle = {"CoincidentDemandSpec", "Reading"};
		List<String> list = Arrays.asList(requiredEle);
		collectAttributes(node, list, map);
		String row = createCSVRowWithAppnder(map);
		ArrayList<String> rows = new ArrayList<>();
		rows.add(row);
		return rows;
	}

	private List<String> getPresentDemandData(Node node, Map<String, String> map) {
		String[] requiredEle = {"PresentDemandSpec", "Reading"};
		List<String> list = Arrays.asList(requiredEle);
		collectAttributes(node, list, map);
		String row = createCSVRowWithAppnder(map);
		ArrayList<String> rows = new ArrayList<>();
		rows.add(row);
		return rows;
	}

	private List<String> getCumulativeDemandData(Node node, Map<String, String> map) {
		String[] requiredEle = {"CumulativeDemandSpec", "Reading"};
		List<String> list = Arrays.asList(requiredEle);
		collectAttributes(node, list, map);
		String row = createCSVRowWithAppnder(map);
		ArrayList<String> rows = new ArrayList<>();
		rows.add(row);
		return rows;
	}

	private List<String> getMaxDemandData(Node node, Map<String, String> map) {
		String[] requiredEle = {"MaxDemandSpec", "Reading"};
		List<String> list = Arrays.asList(requiredEle);
		collectAttributes(node, list, map);
		String row = createCSVRowWithAppnder(map);
		ArrayList<String> rows = new ArrayList<>();
		rows.add(row);
		return rows;
	}

	private List<String> getConsumptionData(Node node, Map<String, String> map) {
		String[] requiredEle = {"ConsumptionSpec", "Reading"};
		List<String> list = Arrays.asList(requiredEle);
		collectAttributes(node, list,map);
		String row = createCSVRowWithAppnder(map);
		ArrayList<String> rows = new ArrayList<>();
		rows.add(row);
		return rows;
	}

	private void collectAttributes(Node node, List<String> list, Map<String, String> map) {
		Element element = (Element) node;
		List<Node> nodes = XmlDomParser.extractAllChildren(element, list);
		map.put("TYPE",node.getNodeName());
		for (Node childNode : nodes){
			String nodeName = childNode.getNodeName();
			if (list.contains(nodeName)){
				Element childElement = (Element) childNode;
				XmlDomParser.getAllAttributes(childElement, map);
			}

		}
		String readingType = getReadingType(map, node.getNodeName());
		map.put("READINGTYPE", readingType);
		map.put("ESTTIME", map.get("TIMESTAMP"));

		System.out.println("attributes "+map);

	}


	//logic to derive the value for reading type
	private String getReadingType(Map<String, String> map, String type){
		StringBuilder readingType = new StringBuilder();
		readingType
				.append(type).append(HYPHEN)
				.append(map.get("DIRECTION")).append(HYPHEN)
				.append(map.get("UOM")).append(HYPHEN)
				.append(map.get("TOUBUCKET")).append(HYPHEN)
				.append(map.get("MEASUREMENTPERIOD"));


		return readingType.toString();
	}

	private String createCSVRow(Map<String, String> map, String[] cols){
		StringBuilder sb = new StringBuilder();
		int lenIncr = 0;
		for (String col : cols){
			lenIncr++;
			sb.append(map.get(col));
			if(cols.length != lenIncr){
				sb.append(SEPERATOR);
			}
		}
		return sb.toString();

	}

	private String createCSVRowWithAppnder(Map<String, String> map){
		StringBuilder sb = new StringBuilder();

		sb.append(map.get("METERNAME")).append(SEPERATOR)
				.append(map.get("TYPE")).append(SEPERATOR)
				.append(map.get("UOM")).append(SEPERATOR)
				.append(map.get("READINGTYPE")).append(SEPERATOR)
				.append(map.get("DIRECTION")).append(SEPERATOR)
				.append(map.get("TOUBUCKET")).append(SEPERATOR)
				.append(map.get("MEASUREMENTPERIOD")).append(SEPERATOR)
				.append(map.get("MULTIPLIER")).append(SEPERATOR)
				.append(map.get("TIMESTAMP")).append(SEPERATOR)
				.append(map.get("ESTTIME")).append(SEPERATOR)
				.append(map.get("VALUE"));

		return sb.toString();
	}

	private String createCSVRowWithAppnderForIntervalData(Map<String, String> map){
		StringBuilder sb = new StringBuilder();


		sb.append(map.get("METERNAME")).append(SEPERATOR)
				.append(map.get("TYPE")).append(SEPERATOR)
				.append(map.get("UOM")).append(SEPERATOR)
				.append(map.get("READINGTYPE")).append(SEPERATOR)
				.append(map.get("DIRECTION")).append(SEPERATOR)
				.append(map.get("INTERVAL")).append(SEPERATOR)
				.append(map.get("MULTIPLIER")).append(SEPERATOR)
				.append(map.get("TIMESTAMP")).append(SEPERATOR)
				.append(map.get("ESTTIME")).append(SEPERATOR)
				.append(map.get("VALUE")).append(SEPERATOR)
				.append(map.get("TIMECHANGED")).append(SEPERATOR)
				.append(map.get("CLOCKSETBACKWARD")).append(SEPERATOR)
				.append(map.get("LONGINTERVAL")).append(SEPERATOR)
				.append(map.get("CLOCKSETFORWARD")).append(SEPERATOR)
				.append(map.get("PARTIALINTERVAL")).append(SEPERATOR)
				.append(map.get("INVALIDTIME")).append(SEPERATOR)
				.append(map.get("SKIPPEDINTERVAL")).append(SEPERATOR)
				.append(map.get("COMPLETEOUTAGE")).append(SEPERATOR)
				.append(map.get("PULSEOVERFLOW")).append(SEPERATOR)
				.append(map.get("TESTMODE")).append(SEPERATOR)
				.append(map.get("TAMPER")).append(SEPERATOR)
				.append(map.get("PARTIALOUTAGE")).append(SEPERATOR)
				.append(map.get("SUSPECTEDOUTAGE")).append(SEPERATOR)
				.append(map.get("RESTORATION")).append(SEPERATOR)
				.append(map.get("DST")).append(SEPERATOR)
				.append(map.get("INVALIDVALUE")).append(SEPERATOR);


		return sb.toString();
	}


	private void writeDataToMap(Map<String, StringBuilder> parentMap, List<String> rows, String nodeName){
		for (String row : rows){
			if(parentMap.get(nodeName) != null){
				StringBuilder sb =parentMap.get(nodeName);
				if(row != null && row.length() > 0)
					sb.append(NEXT_LINE);
				sb.append(row);
			}else{
				StringBuilder sb =new StringBuilder();
				if(row != null && row.length() > 0)
					sb.append(NEXT_LINE);
				sb.append(row);
				parentMap.put(nodeName,sb);
			}
		}
	}

	private void writeCSVDataToOutFile(String nodeName, StringBuilder data, Context context){

		try {
			mos.write(nodeName, NullWritable.get(),new Text(data.substring(1)), nodeName);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private void writeCSVToMapperDrive(String nodeName, StringBuilder data, Context context){

		try {
			if(data.length()>0)
			context.write(new Text(nodeName), new Text(data.toString()));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/*private String createHeader(String[] cols){
		StringBuilder header = new StringBuilder();
		for (String col : cols){
			header.append(SEPERATOR);
			header.append(col);
		}
		return header.toString().substring(1);
	}



	private String createHeader(){
		StringBuilder header = new StringBuilder();
		return header.append("DEVICE_NAME").append(SEPERATOR)
				.append("NODE_NAME").append(SEPERATOR)
				.append("UOM").append(SEPERATOR)
				.append("READING_TYPE").append(SEPERATOR)
				.append("DIRECTION").append(SEPERATOR)
				.append("TOUBUCKET").append(SEPERATOR)
				.append("MEASUREMENTPERIOD").append(SEPERATOR)
				.append("MULTIPLIER").append(SEPERATOR)
				.append("READING_TIME").append(SEPERATOR)
				.append("EST_TIME").append(SEPERATOR)
				.append("READING_VALUE").toString();

	}*/


}