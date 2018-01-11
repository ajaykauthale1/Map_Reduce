/**
 * 
 */
package mapper;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import utils.PageRankCounter;

/**
 * Mapper for parsing records from input BZ2 file and emits records in the following format
 * Z#A~B~C#PR_VALUE
 * where A,B,C are outlinks(adjacency list) of NODE Z
 * and, PR_VALUE is page rank value of the NODE Z
 *
 * @author ajay
 **/
public class ParserMapper extends Mapper<Object, Text, Text, NullWritable> {
	private static Pattern namePattern;
	private static Pattern linkPattern;
	List<String> linkPageNames;
	SAXParserFactory spf = SAXParserFactory.newInstance();
	SAXParser saxParser = null;
	XMLReader xmlReader = null;
	private Set<String> nodeList;
	NullWritable nw = NullWritable.get();
	public void setup(Context ctx){
		
		nodeList = new HashSet<String>();
		
		// Keep only html pages not containing tilde (~).
			namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
			linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");

		// parser setup
			try {
				spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
				saxParser = spf.newSAXParser();
				xmlReader = saxParser.getXMLReader();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// Parser fills this list with linked page names.
			linkPageNames = new LinkedList<String>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));
		
	}
	
	public void map(Object _k, Text record, Context ctx)
			throws InterruptedException, IOException {
		
		String line = record.toString();
		
		int delimLoc = line.indexOf(':');
		String pageName = line.substring(0, delimLoc);
		String html = line.substring(delimLoc + 1);
		Matcher matcher = namePattern.matcher(pageName);
		if (!matcher.find()) {
			// Skip this html file, name contains (~).
			return;
		}

		// Parse page and fill list of linked pages.
		linkPageNames.clear();
		try {
			xmlReader.parse(new InputSource(new StringReader(html)));
			
			StringBuilder output = new StringBuilder();
			output.append(pageName);
			output.append("#");
			String del="";
			for(String oLink : linkPageNames){
				output.append(del);
				del="~";
				output.append(oLink);
			}
			output.append("#");
			output.append(0.0); // default Page Rank value

			// emit records line by line after composing the data in the above mentioned format
			ctx.write(new Text(output.toString()), nw);
			
			//add keys to nodeList to compute total nodes in the data set
			nodeList.add(pageName);
			
		} catch (Exception e) {
			// Discard ill-formatted pages.
			return;
		}
		
	}
	
	public void cleanup(Context ctx){
		// set total nodes in the data set, computed in the counter to be used by job-2
		ctx.getCounter(PageRankCounter.PAGE_COUNTER).setValue(nodeList.size());
	}
	
	
	
	private static class WikiParser extends DefaultHandler {
		/** List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}
	}

}