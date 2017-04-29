package com.demo.reading.parser;

import java.util.*;

import org.w3c.dom.*;

/**
 * Created by hadoop on 19/4/17.
 */
public class XmlDomParser {

    //utitlity mehtod to retrive the data of all child nodes for a element
    public static List<Node> extractAllChildren(Element parentNode) {
        NodeList childNodes = parentNode.getChildNodes();
        List<Node> nodes = new ArrayList<>();
        String result = new String();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE ) {
                result += node.getNodeValue();
                System.out.println(node.getNodeName());
                nodes.add(node);
            }
        }
        return nodes;
    }

    public static List<Node> extractAllChildren(Element parentNode, List<String> requiredNodes) {
        NodeList childNodes = parentNode.getChildNodes();
        List<Node> nodes = new ArrayList<>();
        String result = new String();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE && requiredNodes.contains(node.getNodeName()) ) {
                result += node.getNodeValue();
                System.out.println(node.getNodeName());
                nodes.add(node);
            }
        }
        return nodes;
    }

    public static Node getFirstChild(Element parentNode, String name){
        System.out.println("*****************getting the first child");
        NodeList childNodes = parentNode.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE && name.equals(node.getNodeName())) {
                return node;
            }

        }
        return null;
    }

    //utitlity mehtod to retrive the attributes for a element
    public static Map<String, String> getAllAttributes(Element element) {
        Map<String,String> allAttributes = new HashMap<String,String>();
        System.out.println("List attributes for node: " + element.getNodeName());
        NamedNodeMap attributes = element.getAttributes();
        int numAttrs = attributes.getLength();
        for (int i = 0; i < numAttrs; i++) {
            Attr attr = (Attr) attributes.item(i);
            String attrName = attr.getNodeName();
            String attrValue = attr.getNodeValue();
            allAttributes.put(attrName.toUpperCase(), attrValue);
            System.out.println("Found attribute: " + attrName + " with value: " + attrValue);
        }
        return allAttributes;
    }

    public static void getAllAttributes(Element element, Map<String,String> map) {
        NamedNodeMap attributes = element.getAttributes();
        int numAttrs = attributes.getLength();
        for (int i = 0; i < numAttrs; i++) {
            Attr attr = (Attr) attributes.item(i);
            String attrName = attr.getNodeName();
            String attrValue = attr.getNodeValue();
            map.put(attrName.toUpperCase(), attrValue);
            System.out.println("Found attribute: " + attrName + " with value: " + attrValue);
        }

    }

    public static String getAttributeByName(Element eElement, String name){
        return eElement.getAttribute(name);
    }

    public static String getAttributeByName(Node node, String name){
        Element element = (Element) node;
        return element.getAttribute(name);

    }


    public static void visitRecursively(Node node, Map<String, String> map) {
        Element element = (Element) node;
        getAllAttributes(element, map);
        NodeList list = node.getChildNodes();
        for (int i=0; i<list.getLength(); i++) {
            if (node.getNodeType() == Node.ELEMENT_NODE){
                Node childNode = list.item(i);
                visitRecursively(childNode, map);
            }

        }
    }



}
