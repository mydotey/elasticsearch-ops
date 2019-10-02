package org.mydotey.tool.elasticsearch.ops.state;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.NodeMetaData;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardStateMetaData;

/**
 * @author koqizhao
 *
 * Sep 28, 2019
 */
public class StateMetaData<T> {
    
    public static final String STATE_DIR_NAME = "_state";
    public static final String STATE_FILE_EXTENSION = ".st";

    public static final String STATE_FILE_CODEC = "state";
    public static final int MIN_COMPATIBLE_STATE_FILE_VERSION = 1;
    public static final int STATE_FILE_VERSION = 1;

    public static final NamedXContentRegistry CLUSTER_REGISTRY = new NamedXContentRegistry(
        ClusterModule.getNamedXWriteables());

    private MetaDataStateFormat<T> _format;
    private T _metaData;

    public StateMetaData(MetaDataStateFormat<T> format, String file) throws IOException {
        this(format, format.read(NamedXContentRegistry.EMPTY, Paths.get(file)));
    }

    public StateMetaData(MetaDataStateFormat<T> format, NamedXContentRegistry namedXContentRegistry,
        String file) throws IOException {
        this(format, format.read(namedXContentRegistry, Paths.get(file)));
    }

    public StateMetaData(MetaDataStateFormat<T> format, T metaData) throws IOException {
        _format = format;
        _metaData = metaData;
    }

    public T getMetaData() {
        return _metaData;
    }

    public String toJson() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        _format.toXContent(builder, _metaData);
        builder.endObject();
        return Strings.toString(builder);
    }

    public void saveTo(String file) throws IOException {
        try (IndexOutput out = new OutputStreamIndexOutput("state", "state", new FileOutputStream(file), 1024)) {
            CodecUtil.writeHeader(out, STATE_FILE_CODEC, STATE_FILE_VERSION);
            out.writeInt(MetaDataStateFormat.FORMAT.index());
            try (XContentBuilder builder = XContentFactory.contentBuilder(
                MetaDataStateFormat.FORMAT, new IndexOutputOutputStream(out) {
                @Override
                public void close() {
                }
            })) {
                builder.startObject();
                _format.toXContent(builder, _metaData);
                builder.endObject();
            }
            CodecUtil.writeFooter(out);
        }
    }

    public static StateMetaData<MetaData> newClusterMetaData(String metadataFile) throws IOException {
        return new StateMetaData<MetaData>(MetaData.FORMAT, CLUSTER_REGISTRY, metadataFile);
    }

    public static StateMetaData<MetaData> newClusterMetaData(MetaData metadata) throws IOException {
        return new StateMetaData<MetaData>(MetaData.FORMAT, metadata);
    }

    public static StateMetaData<NodeMetaData> newNodeMetaData(String metadataFile) throws IOException {
        return new StateMetaData<NodeMetaData>(NodeMetaData.FORMAT, metadataFile);
    }

    public static StateMetaData<NodeMetaData> newNodeMetaData(NodeMetaData metaData) throws IOException {
        return new StateMetaData<NodeMetaData>(NodeMetaData.FORMAT, metaData);
    }

    public static StateMetaData<Manifest> newManifestMetaData(String metadataFile) throws IOException {
        return new StateMetaData<Manifest>(Manifest.FORMAT, metadataFile);
    }

    public static StateMetaData<Manifest> newManifestMetaData(Manifest metaData) throws IOException {
        return new StateMetaData<Manifest>(Manifest.FORMAT, metaData);
    }

    public static StateMetaData<IndexMetaData> newIndexMetaData(String metadataFile) throws IOException {
        return new StateMetaData<IndexMetaData>(IndexMetaData.FORMAT, metadataFile);
    }

    public static StateMetaData<IndexMetaData> newIndexMetaData(IndexMetaData metaData) throws IOException {
        return new StateMetaData<IndexMetaData>(IndexMetaData.FORMAT, metaData);
    }

    public static StateMetaData<ShardStateMetaData> newShardMetaData(String metadataFile) throws IOException {
        return new StateMetaData<ShardStateMetaData>(ShardStateMetaData.FORMAT, metadataFile);
    }

    public static StateMetaData<ShardStateMetaData> newShardMetaData(ShardStateMetaData metaData) throws IOException {
        return new StateMetaData<ShardStateMetaData>(ShardStateMetaData.FORMAT, metaData);
    }

    public static final MetaDataStateFormat<RetentionLeases> RETENTION_LEASES_STATE_FORMAT =
        new MetaDataStateFormat<RetentionLeases>("retention-leases-") {
        @Override
        public void toXContent(final XContentBuilder builder, final RetentionLeases retentionLeases)
            throws IOException {
            retentionLeases.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }

        @Override
        public RetentionLeases fromXContent(final XContentParser parser) {
            return RetentionLeases.fromXContent(parser);
        }
    };

    public static StateMetaData<RetentionLeases> newRetentionLeasesMetaData(String metadataFile)
        throws IOException {
        return new StateMetaData<RetentionLeases>(RETENTION_LEASES_STATE_FORMAT, metadataFile);
    }

    public static StateMetaData<RetentionLeases> newRetentionLeasesMetaData(RetentionLeases metaData)
        throws IOException {
        return new StateMetaData<RetentionLeases>(RETENTION_LEASES_STATE_FORMAT, metaData);
    }
}
