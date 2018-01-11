/**
 * 
 */
package util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * @author Ajay
 *
 */
public class LoaderRoutine {
	/** Current user directory */
	public static String ROOT_DIR = "resources";

	public static List<String> loadFile(String filename) {
		byte[] buffer = new byte[1024];
		List<String> lines = new LinkedList<String>();

		try {
			GZIPInputStream gzip = new GZIPInputStream(
					new FileInputStream(LoaderRoutine.ROOT_DIR + "\\input\\" + filename));
			FileOutputStream csv = new FileOutputStream(LoaderRoutine.ROOT_DIR + "\\output\\input.csv");

			int length;

			while ((length = gzip.read(buffer)) > 0) {
				csv.write(buffer, 0, length);
			}

			gzip.close();
			csv.close();

			BufferedReader reader = new BufferedReader(new FileReader(LoaderRoutine.ROOT_DIR + "\\output\\input.csv"));
			String line = "";
			
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return lines;
	}
}
