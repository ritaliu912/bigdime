/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.constants;

public final class HiveClientConstants {
	private HiveClientConstants(){}
	public static final String DB_CREATE="createDatabase";
	public static final String DB_DROP="dropDatabase";
	public static final String ADD_PARTITION="addPartition";
	public static final String DROP_PARTITION="dropPartition";
	
	public static final String DFS_CLIENT_FAILOVER_PROVIDER = "dfs.client.failover.proxy.provider.haservicename";
	public static final String DFS_NAME_SERVICES = "dfs.nameservices";
	public static final String DFS_HA_NAMENODES  = "dfs.ha.namenodes.haservicename";
	public static final String DFS_NAME_NODE_LIST = "nn1,nn2";
	public static final String DFS_NAME_NODE_RPC_ADDRESS_NODE1 = "dfs.namenode.rpc-address.haservicename.nn1";
	public static final String DFS_NAME_NODE_RPC_ADDRESS_NODE2 = "dfs.namenode.rpc-address.haservicename.nn2";
	public static final String HA_SERVICE_NAME = "haservicename";
	public static final String HA_ENABLED = "ha.enable";
	
	public static final String TYPE_STRING = "STRING";
	public static final String TYPE_NUMBER = "NUMBER";
	public static final String TYPE_DATE = "DATE";
	public static final String TYPE_VARCHAR2 = "VARCHAR2";
	public static final String TYPE_TIMESTAMP = "TIMESTAMP";
	public static final String TYPE_INT = "INT";
	public static final String TYPE_BOOLEAN = "BOOLEAN";
	public static final String TYPE_RAW = "RAW";
}
