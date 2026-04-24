/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

// The base class formats the signal table as `catalog.schema.table` (3-part, dot-separated),
// but Informix's JDBC driver rejects that: the first dot is read as a cross-database qualifier
// and the driver fails with "Database (<catalog>) not found". Informix's native cross-database
// syntax `catalog:schema.table` also fails in prepared statements because the driver treats
// `:` as a named-parameter marker.
//
// The connector's JDBC session is already bound to the target database via `database.dbname`,
// so no cross-database qualification is needed here. Emitting `schema.table` (Informix's
// `owner.table`) lets the server resolve the signal table in the current database.
public class InformixSignalBasedIncrementalSnapshotChangeEventSource<P extends Partition, T extends DataCollectionId>
        extends SignalBasedIncrementalSnapshotChangeEventSource<P, T> {

    public InformixSignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                   JdbcConnection jdbcConnection,
                                                                   EventDispatcher<P, T> dispatcher,
                                                                   DatabaseSchema<?> databaseSchema,
                                                                   Clock clock,
                                                                   SnapshotProgressListener<P> progressListener,
                                                                   DataChangeEventListener<P> dataChangeEventListener,
                                                                   NotificationService<P, ? extends OffsetContext> notificationService) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
    }

    @Override
    protected String getSignalTableName(String dataCollectionId) {
        if (Strings.isNullOrEmpty(dataCollectionId)) {
            return dataCollectionId;
        }
        TableId tableId = TableId.parse(dataCollectionId);
        // TableId.parse maps a 2-part string to (catalog=first, schema=null, table=second)
        // and a 3-part string to (catalog=first, schema=second, table=third). Either way,
        // the Informix owner we want is whichever of those fields is non-null.
        String owner = tableId.schema() != null ? tableId.schema() : tableId.catalog();
        return owner + "." + tableId.table();
    }
}