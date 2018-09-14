package com.flipkart.yak.sep;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class SepConfigDeserializeTestCase {
	
	
	@Test
	public void testReadingConf() throws IOException {
		
		ObjectMapper mapper = new ObjectMapper();
		mapper.readValue(new File("src/test/resources/sep-conf.json"), SepConfig.class);
	}
}
