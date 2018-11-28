/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.nexus.db.pool;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.sql.ExceptionCheck;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;

/**
 * HikariCP Connection Manager.
 *
 * @author dstipp
 */
public class HikariCPConnectionManager extends BaseConnectionManager {
    private static final Logger log = LoggerFactory.getLogger(HikariCPConnectionManager.class);

    private ConnectionConfig connConfig;

    private enum StateType {
        // Base state at construction.  Nothing is set.
        ZERO,

        // "Normal" state.  dataSourcePool and poolProxy are set.
        NORMAL,

        // Closed state.  closeTrace is set.
        CLOSED;
    }

    private static class State {
        final StateType type;
        final HikariDataSource dataSourcePool;
        final HikariPoolMXBean poolProxy;
        final Throwable closeTrace;

        public State(StateType type, HikariDataSource dataSourcePool, HikariPoolMXBean poolProxy, Throwable closeTrace) {
            this.type = type;
            this.dataSourcePool = dataSourcePool;
            this.poolProxy = poolProxy;
            this.closeTrace = closeTrace;
        }
    }

    private volatile State state = new State(StateType.ZERO, null, null, null);

    public HikariCPConnectionManager(ConnectionConfig connConfig) {
        this.connConfig = Preconditions.checkNotNull(connConfig);
    }

    @Override
    public Connection getConnection() throws SQLException {
        ConnectionAcquisitionProfiler profiler = new ConnectionAcquisitionProfiler();
        try {
            return acquirePooledConnection(profiler);
        } finally {
            profiler.logQueryDuration();
        }
    }

    /**
     * Attempts to acquire a valid database connection from the pool.
     * @param profiler Connection acquisition profiler for this attempt to acquire a connection
     * @return valid pooled connection
     * @throws SQLException if a connection cannot be acquired within the {@link ConnectionConfig#getCheckoutTimeout()}
     */
    private Connection acquirePooledConnection(ConnectionAcquisitionProfiler profiler) throws SQLException {
        while (true) {
            HikariDataSource dataSourcePool = checkAndGetDataSourcePool();
            Connection conn = dataSourcePool.getConnection();
            profiler.logAcquisitionAndRestart();

            try {
                testConnection(conn, Optional.of(profiler));
                return conn;
            } catch (SQLException e) {
                log.error("[{}] Dropping connection which failed validation",
                        connConfig.getConnectionPoolName(),
                        e);
                dataSourcePool.evictConnection(conn);

                if (profiler.globalStopwatch.elapsed(TimeUnit.MILLISECONDS) > connConfig.getCheckoutTimeout()) {
                    // It's been long enough that had hikari been
                    // validating internally it would have given up rather
                    // than retry
                    throw e;
                }
            }
        }
    }

    private void logPoolStats() {
        if (log.isWarnEnabled()) {
            State stateLocal = state;
            HikariPoolMXBean poolProxy = stateLocal.poolProxy;
            if (poolProxy != null) {
                try {
                    log.warn(
                            "[{}] HikariCP: "
                                    + "numBusyConnections = {}, "
                                    + "numIdleConnections = {}, "
                                    + "numConnections = {}, "
                                    + "numThreadsAwaitingCheckout = {}",
                            connConfig.getConnectionPoolName(),
                            poolProxy.getActiveConnections(),
                            poolProxy.getIdleConnections(),
                            poolProxy.getTotalConnections(),
                            poolProxy.getThreadsAwaitingConnection());
                } catch (Exception e) {
                    log.error("[{}] Unable to log pool statistics.", connConfig.getConnectionPoolName(), e);
                }
            }
        }
    }

    private HikariDataSource checkAndGetDataSourcePool() throws SQLException {
        while (true) {
            // Volatile read state to see if we can get through here without locking.
            State stateLocal = state;

            switch (stateLocal.type) {
                case ZERO:
                    // Not yet initialized, no such luck.
                    synchronized (this) {
                        if (state == stateLocal) {
                            // The state hasn't changed on us, we can perform
                            // the initialization and start over again.
                            state = normalState();
                        } else {
                            // Someone else changed the state on us, just start over.
                        }
                    }
                    break;

                case NORMAL:
                    // Normal state, good to go
                    return stateLocal.dataSourcePool;

                case CLOSED:
                    throw new SQLException("Hikari connection pool already closed!", stateLocal.closeTrace);
            }

            // fall throughs are spins
        }
    }

    private void logConnectionFailure() {
        log.error("Failed to get connection from the datasource "
                        + "{}. Please check the jdbc url ({}), the password, "
                        + "and that the secure server key is correct for the hashed password.",
                connConfig.getConnectionPoolName(),
                connConfig.getUrl());
    }

    /**
     * Test an initialized dataSourcePool by running a simple query.
     * @param dataSourcePool data source to validate
     * @throws SQLException if data source is invalid and cannot be used.
     */
    private void testDataSource(HikariDataSource dataSourcePool) throws SQLException {
        try {
            try (Connection conn = dataSourcePool.getConnection()) {
                testConnection(conn);
            }
        } catch (SQLException e) {
            logConnectionFailure();
            throw e;
        }
    }

    /**
     * Test a database connection to determine if it is valid for use.
     * @param connection connection to validate
     * @throws SQLException if connection is invalid and cannot be used.
     */
    private void testConnection(Connection connection) throws SQLException {
        testConnection(connection, Optional.empty());
    }

    private void testConnection(
            Connection connection, Optional<ConnectionAcquisitionProfiler> profiler) throws SQLException {
        try (Statement stmt = createStatementAndLogTime(connection, profiler);
                ResultSet rs = executeQueryAndLogTime(stmt, profiler)) {
            if (!rs.next()) {
                throw new SQLException(String.format(
                        "Connection %s could not be validated as it did not return any results for test query %s",
                        connection, connConfig.getTestQuery()));
            }
        }
    }

    private Statement createStatementAndLogTime(
            Connection connection, Optional<ConnectionAcquisitionProfiler> profiler) throws SQLException {
        Statement statement = connection.createStatement();
        profiler.ifPresent(ConnectionAcquisitionProfiler::logStatementCreationAndRestart);
        return statement;
    }

    private ResultSet executeQueryAndLogTime(
            Statement stmt, Optional<ConnectionAcquisitionProfiler> profiler) throws SQLException {
        ResultSet resultSet = stmt.executeQuery(connConfig.getTestQuery());
        profiler.ifPresent(ConnectionAcquisitionProfiler::logQueryExecution);
        return resultSet;
    }

    @Override
    public synchronized void close() {
        try {
            switch (state.type) {
                case ZERO:
                    break;

                case NORMAL:
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Closing connection pool: {}",
                                connConfig,
                                new RuntimeException("Closing connection pool"));
                    }

                    state.dataSourcePool.close();
                    break;

                case CLOSED:
                    break;
            }
        } finally {
            state = new State(StateType.CLOSED, null, null, new Throwable("Hikari pool closed here"));
        }
    }

    /**
     * Initializes a connection to the provided database.
     */
    @Override
    public synchronized void init() throws SQLException {
        if (state.type == StateType.ZERO) {
            state = normalState();
        }
    }

    private State normalState() throws SQLException {
        HikariDataSource dataSourcePool = getDataSourcePool();
        boolean keep = false;

        try {
            // Setup monitoring
            HikariPoolMXBean poolProxy = initPoolMbeans();
            testDataSource(dataSourcePool);
            keep = true;
            return new State(StateType.NORMAL, dataSourcePool, poolProxy, null);
        } finally {
            if (!keep) {
                IOUtils.closeQuietly(dataSourcePool);
            }
        }
    }

    private HikariDataSource getDataSourcePool() {
        // Print a stack trace whenever we initialize a pool
        log.debug("Initializing connection pool: {}", connConfig, new RuntimeException("Initializing connection pool"));

        HikariDataSource dataSourcePool;

        try {
            try {
                dataSourcePool = new HikariDataSource(connConfig.getHikariConfig());
            } catch (IllegalArgumentException e) {
                // allow multiple pools on same JVM (they need unique names / endpoints)
                if (e.getMessage().contains("A metric named")) {
                    String poolName = connConfig.getConnectionPoolName();

                    connConfig.getHikariConfig().setPoolName(poolName + "-" + ThreadLocalRandom.current().nextInt());
                    dataSourcePool = new HikariDataSource(connConfig.getHikariConfig());
                } else {
                    throw e;
                }
            }
        } catch (PoolInitializationException e) {
            log.error("[{}] Failed to initialize hikari data source: {}",
                    connConfig.getConnectionPoolName(), connConfig.getUrl(), e);

            if (ExceptionCheck.isTimezoneInvalid(e)) {
                String tzname = TimeZone.getDefault().getID();

                final String errorPreamble = "Invalid system timezone " + tzname + " detected.";
                final String errorAfterward = "This can be corrected at the system level or overridden in the JVM startup flags by appending -Duser.timezone=UTC. Contact support for more information.";

                if (tzname.equals("Etc/Universal")) {
                    // Really common failure case. UTC is both a name AND an abbreviation.
                    log.error(
                            "{} The timezone *name* should be UTC. {}",
                            errorPreamble,
                            errorAfterward);
                } else {
                    log.error(
                            "{} This is caused by using non-standard or unsupported timezone names. {}",
                            errorPreamble,
                            errorAfterward);
                }
            }

            // This exception gets thrown by HikariCP and is useless outside of ConnectionManagers.
            RuntimeException e2 = new RuntimeException(ExceptionUtils.getMessage(e), e.getCause());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
        }
        return dataSourcePool;
    }

    /**
     * Setup JMX client.  This is only used for trace logging.
     */
    private HikariPoolMXBean initPoolMbeans() {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        ObjectName poolName = null;
        try {
            poolName = new ObjectName("com.zaxxer.hikari:type=Pool (" + connConfig.getConnectionPoolName() + ")");
        } catch (MalformedObjectNameException e) {
            log.error("Unable to setup mBean monitoring for pool {}.", connConfig.getConnectionPoolName(), e);
        }
        return JMX.newMBeanProxy(mBeanServer, poolName, HikariPoolMXBean.class);
    }

    @Override
    public DBType getDbType() {
        return connConfig.getDbType();
    }

    private class ConnectionAcquisitionProfiler {
        private static final String PROFILING_MESSAGE_FORMAT = "[{}] Waited {}ms for connection"
                + " (acquisition: {}, statement creation: {}, execution: {})";

        private final Stopwatch globalStopwatch;
        private final Stopwatch trialStopwatch;

        private long acquisitionMillis;
        private long statementCreationMillis;
        private long queryExecutionMillis;

        private ConnectionAcquisitionProfiler() {
            this(Stopwatch.createStarted(), Stopwatch.createStarted());
        }

        private ConnectionAcquisitionProfiler(Stopwatch globalStopwatch, Stopwatch trialStopwatch) {
            this.globalStopwatch = globalStopwatch;
            this.trialStopwatch = trialStopwatch;
        }

        private void logAcquisitionAndRestart() {
            trialStopwatch.stop();
            acquisitionMillis = trialStopwatch.elapsed(TimeUnit.MILLISECONDS);
            trialStopwatch.reset().start();
        }

        private void logStatementCreationAndRestart() {
            trialStopwatch.stop();
            statementCreationMillis = trialStopwatch.elapsed(TimeUnit.MILLISECONDS);
            trialStopwatch.reset().start();
        }

        private void logQueryExecution() {
            trialStopwatch.stop();
            queryExecutionMillis = trialStopwatch.elapsed(TimeUnit.MILLISECONDS);
        }

        private void logQueryDuration() {
            long elapsedMillis = globalStopwatch.elapsed(TimeUnit.MILLISECONDS);
            if (elapsedMillis > 1000) {
                log.warn(PROFILING_MESSAGE_FORMAT,
                        connConfig.getConnectionPoolName(),
                        elapsedMillis,
                        acquisitionMillis,
                        statementCreationMillis,
                        queryExecutionMillis);
                logPoolStats();
            } else {
                log.debug(PROFILING_MESSAGE_FORMAT,
                        connConfig.getConnectionPoolName(),
                        elapsedMillis,
                        acquisitionMillis,
                        statementCreationMillis,
                        queryExecutionMillis);
            }
        }
    }
}
