package com.xxl.job.executor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.ConsistencyLevel;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;

import tsinghua.thss.sdk.core.Client;
import tsinghua.thss.sdk.core.KMXClient;
import tsinghua.thss.sdk.core.KMXConfig;

@Configuration
public class KmxConfig {
	@Value("${kmxIp}")
	private String kmxIp;
	@Value("${kmxPort}")
	private String kmxPort;
	@Bean(name="KmxClient")
	public Client getClient() throws NoHostAvailableException, QueryExecutionException, TimeoutException {
		 KMXConfig config = KMXConfig.builder()
				 	.setClusterKeySpace("cty")
				 	.setClusterNodes(kmxIp.split(","))
	                .setClusterPort(Integer.parseInt(kmxPort))
	                .setCoreConnectionsPerHost(3)
	                .setMaxConnectionsPerHost(10)
	                .setMaxRequestsPerConnection(4096)
	                .setHeartbeatIntervalSeconds(0)
	                .setTimeoutMillis(12000)
	                .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
	                .setAutoBatch(false)
	                .build();
	        Client client = new KMXClient(config);
	        return client;
	}
}
