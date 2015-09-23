package com.aerolush.test;

import com.aerolush.lucene.store.FileSegment;
import com.aerospike.client.Host;
import com.spikeify.Spikeify;
import com.spikeify.SpikeifyService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Aerospike {

	public static final String DEFAULT_NAMESPACE = "test";

	public static final String DEFAULT_HOST = "127.0.0.1";
	private static final Integer DEFAULT_PORT = 3000;

	final Logger log = Logger.getLogger(Aerospike.class.getSimpleName());

	/**
	 * This method brings Spikefy service up to speed
	 * ... can be called multiple times
	 * ... but will execute only once (and once it should be enough)
	 */
	public Spikeify getSfy() {

		if (SpikeifyService.getClient() == null) {
			log.info("Starting Aerospike initialization...");
			Map<String, Integer> hosts;

			log.info("--== LOCALHOST ==--");
			hosts = new HashMap<>();
			hosts.put(DEFAULT_HOST, DEFAULT_PORT);

			String namespace = DEFAULT_NAMESPACE;
			log.info("Aerospike default namespace: " + namespace);

			List<Host> hostsData = hosts.entrySet().stream().map(stringIntegerEntry -> new Host(stringIntegerEntry.getKey(), stringIntegerEntry.getValue())).collect(Collectors.toList());
			SpikeifyService.globalConfig(namespace, hostsData.toArray(new Host[hostsData.size()]));

			log.info("Aerospike configured.");

			log.info("Creating indexes...");

			SpikeifyService.register(FileSegment.class);

			/*SpikeifyService.getClient().createIndex(new Policy(), namespace, File.class.getSimpleName(), "fileName", "fileName", IndexType.STRING);
			SpikeifyService.getClient().createIndex(new Policy(), namespace, FileLock.class.getSimpleName(), "lockName", "lockName", IndexType.STRING);
*/
			log.info("End of creating indexes.");

		}

		return SpikeifyService.sfy();
	}

}

