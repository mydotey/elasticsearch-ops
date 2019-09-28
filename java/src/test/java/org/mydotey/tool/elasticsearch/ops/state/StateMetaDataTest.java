package org.mydotey.tool.elasticsearch.ops.state;

import java.io.IOException;
import java.util.HashMap;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.env.NodeMetaData;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardStateMetaData;
import org.junit.Test;

public class StateMetaDataTest {

    public static final String DATA_PATH =
        "/home/koqizhao/Projects/mydotey/elasticsearch/elasticsearch-ops/data/es-data/";
    public static final String CLUSTER_DATA_PATH = DATA_PATH + "nodes/0/_state/";
    public static final String CLUSTAR_STATE_FILE = CLUSTER_DATA_PATH + "global-7.st";
    public static final String NODE_STATE_FILE = CLUSTER_DATA_PATH + "node-5.st";
    public static final String MANIFEST_STATE_FILE = CLUSTER_DATA_PATH + "manifest-35.st";

    public static final String INDEX_DATA_PATH = DATA_PATH + "nodes/0/indices/buIw3-JNR1-nPZmXBPa7IA/_state/";
    public static final String INDEX_STATE_FILE = INDEX_DATA_PATH + "state-12.st";

    public static final String SHARD_DATA_PATH = DATA_PATH + "nodes/0/indices/buIw3-JNR1-nPZmXBPa7IA/0/_state/";
    public static final String SHARD_STATE_FILE = SHARD_DATA_PATH + "state-3.st";
    public static final String RETENTION_LEASES_STATE_FILE = SHARD_DATA_PATH + "retention-leases-3.st";

    @Test
    public void loadAndPrint() throws IOException {
        StateMetaData<MetaData> clusterMetaData =
            StateMetaData.newClusterMetaData(CLUSTAR_STATE_FILE);
        print(clusterMetaData);

        StateMetaData<NodeMetaData> nodeMetaData =
            StateMetaData.newNodeMetaData(NODE_STATE_FILE);
        print(nodeMetaData);

        StateMetaData<Manifest> manifestMetaData =
            StateMetaData.newManifestMetaData(MANIFEST_STATE_FILE);
        print(manifestMetaData);

        StateMetaData<IndexMetaData> indexMetaData =
            StateMetaData.newIndexMetaData(INDEX_STATE_FILE);
        print(indexMetaData);

        StateMetaData<ShardStateMetaData> shardMetaData =
            StateMetaData.newShardMetaData(SHARD_STATE_FILE);
        print(shardMetaData);

        StateMetaData<RetentionLeases> retentionLeasesMetaDataPryinter =
            StateMetaData.newRetentionLeasesMetaData(RETENTION_LEASES_STATE_FILE);
        print(retentionLeasesMetaDataPryinter);

        String manifestFile2 = "/home/koqizhao/tmp/m.st";
        Manifest manifest = manifestMetaData.getMetaData();
        manifest = new Manifest(manifest.getCurrentTerm(), manifest.getClusterStateVersion(), manifest.getGlobalGeneration(), 
            new HashMap<Index, Long>(manifest.getIndexGenerations()));
        manifest.getIndexGenerations().put(new Index("xx", "xxxx"), 5L);
        manifestMetaData = StateMetaData.newManifestMetaData(manifest);
        manifestMetaData.saveTo(manifestFile2);
        manifestMetaData =
            StateMetaData.newManifestMetaData(manifestFile2);
        print(manifestMetaData);
    }

    private static <T> void print(StateMetaData<T> metaData) throws IOException {
        System.out.printf("\nState:\n%s\n\n", metaData.toJson());
    }

}
