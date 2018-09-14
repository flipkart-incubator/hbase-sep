package com.flipkart.yak.sep;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Abstracts specific column family config if any
 * @author gokulvanan.v
 *
 */
public class CFConfig {

	private Map<String,Object> kafkaConfig; 
	private Set<String> whiteListedQualifier = new HashSet<>();
	private Map<String,String> qualifierToTopicNameMap = new HashMap<>();
	

	public Map<String, Object> getKafkaConfig() {
		return kafkaConfig;
	}
	public void setKafkaConfig(Map<String, Object> kafkaConfig) {
		this.kafkaConfig = kafkaConfig;
	}
	
	public Set<String> getWhiteListedQualifier() {
		return whiteListedQualifier;
	}
	public void setWhiteListedQualifier(Set<String> whiteListedQualifier) {
		this.whiteListedQualifier = whiteListedQualifier;
	}
	public Map<String, String> getQualifierToTopicNameMap() {
		return qualifierToTopicNameMap;
	}
	public void setQualifierToTopicNameMap(Map<String, String> qualifierToTopicNameMap) {
		this.qualifierToTopicNameMap = qualifierToTopicNameMap;
	}
	
}
