package org.recxx.utils;

public class StringUtils {

	private StringUtils() {
	}

	public static boolean isNullOrEmpty(String string) {
		return string == null || string.equals("");
	}
}
