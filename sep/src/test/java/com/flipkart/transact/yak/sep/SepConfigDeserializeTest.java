package com.flipkart.transact.yak.sep;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.flipkart.yak.sep.SepConfig;

public class SepConfigDeserializeTest {
	
	
	@Test
	public void testReadingConf() throws JsonParseException, JsonMappingException, IOException {
		
		ObjectMapper mapper = new ObjectMapper();
		SepConfig conf = mapper.readValue(new File("src/test/resources/sep-conf.json"), SepConfig.class);
		
		
	}

}
