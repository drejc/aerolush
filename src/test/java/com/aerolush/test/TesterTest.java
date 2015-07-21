package com.aerolush.test;

import org.junit.*;

import java.io.IOException;

public class TesterTest {

	@Test
	public void test() throws IOException {

		Tester tester = new Tester();
		tester.init();
	}
}