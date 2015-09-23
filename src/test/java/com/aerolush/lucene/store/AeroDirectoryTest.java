package com.aerolush.lucene.store;

import com.aerolush.test.Aerospike;
import org.apache.lucene.store.Directory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class AeroDirectoryTest {

	Aerospike aerospike = new Aerospike();

	@Before
	public void setUp() {

		aerospike.getSfy().truncateNamespace("test");
	}


	@Test
	public void renameFileTest() throws IOException {

		Directory directory = new AeroDirectory(aerospike.getSfy());

		// prepare
		File old = new File("oldName");
		aerospike.getSfy().create(old).now();

		String[] list = directory.listAll();
		assertEquals(1, list.length);
		assertEquals("oldName", list[0]);

		// rename
		directory.renameFile("oldName", "newName");

		// check
		list = directory.listAll();
		assertEquals(1, list.length);
		assertEquals("newName", list[0]);
	}
}