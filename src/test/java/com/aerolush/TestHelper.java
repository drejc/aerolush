package com.aerolush;

import java.io.*;

public final class TestHelper {

	public static String getResource(Class clazz, String fileName) {
		try {
			File file = new File(clazz.getResource(fileName).getFile());
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF8"));

			StringBuilder builder = new StringBuilder();
			String str;
			while ((str = in.readLine()) != null) {
				builder.append(str);
			}

			in.close();

			return builder.toString();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}
}
