package com.aerolush;

import com.aerolush.lucene.store.AeroDirectory;
import com.aerolush.test.Aerospike;
import com.spikeify.Spikeify;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class FullTextSearchTest {

	private static final String TEXT_FILE = "/AdventuresOfHuckleberryFinn.txt";

	private final Aerospike spike;

	public FullTextSearchTest() {

		spike = new Aerospike();
	}

	private Spikeify getSfy() {

		return spike.getSfy();
	}

	@Before
	public void prepareIndex() throws IOException {

		getSfy().truncateNamespace("test");

		System.out.println("Reading: " + TEXT_FILE);
		// load large set of text data and create and index ...
		File file = new File(getClass().getResource(TEXT_FILE).getFile());
		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF8"));

		// create index
		StandardAnalyzer analyzer = new StandardAnalyzer();

		Directory index = new AeroDirectory(getSfy());
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		IndexWriter w = new IndexWriter(index, config);

		long startTime = System.currentTimeMillis();

		String line;
		long lineNumber = 0;

		while ((line = in.readLine()) != null) {

			lineNumber ++;

			// read the file line by line and build up index in Aerolush
			// additionally count selected words to have data for search checks
			if (line.trim().length() > 0) {

				addDoc(w, line, lineNumber);
			}
		}

		w.close();
		in.close();
		index.close();

		long timeSpent = System.currentTimeMillis() - startTime;
		System.out.println("Prepared index from: " + TEXT_FILE + ", in: " + timeSpent/1000 + "s");
	}

	/**
	 * storing two things here ... the whole line ... and the line number
 	 */
	private static void addDoc(IndexWriter w, String text, long lineNumber) throws IOException {
		Document doc = new Document();
		doc.add(new TextField("text", text, Field.Store.YES));
		doc.add(new LongField("line", lineNumber, Field.Store.YES));
		w.addDocument(doc);
	}

	@Test
	public void testSearch() throws IOException {

		StandardAnalyzer analyzer = new StandardAnalyzer();
		Query q = null;
		try {
			q = new QueryParser("text", analyzer).parse("gutenberg");
		} catch (org.apache.lucene.queryparser.classic.ParseException e) {
			e.printStackTrace();
		}

		// 3. search
		int hitsPerPage = 100;

		Directory index = new AeroDirectory(getSfy());
		IndexReader reader = DirectoryReader.open(index);
		IndexSearcher searcher = new IndexSearcher(reader);

		TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
		searcher.search(q, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;

		// 4. display results
		System.out.println("FOUND: " + hits.length);
		System.out.println("*************************");
		for (int i = 0; i < hits.length; ++i) {

			int docId = hits[i].doc;
			Document d = searcher.doc(docId);
			System.out.println((i + 1) + ". " + d.get("line") + "\t" + d.get("text"));
		}
		System.out.println("*************************");

		// reader can only be closed when there
		// is no need to access the documents any more.
		reader.close();
	}
}
