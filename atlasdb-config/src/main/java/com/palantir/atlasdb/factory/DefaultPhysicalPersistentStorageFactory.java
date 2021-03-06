/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.factory;

import java.io.File;
import java.util.UUID;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.config.RocksDbPersistentStorageConfig;
import com.palantir.atlasdb.persistent.api.PhysicalPersistentStore;
import com.palantir.atlasdb.persistent.rocksdb.RocksDbPhysicalPersistentStore;

/**
 * Constructs a new {@link PhysicalPersistentStore} with new persistent storage connection on each call of
 * {@link DefaultPhysicalPersistentStorageFactory#constructPersistentStore(RocksDbPersistentStorageConfig)}.
 */
public final class DefaultPhysicalPersistentStorageFactory implements PhysicalPersistentStorageFactory {
    private static final Logger log = LoggerFactory.getLogger(DefaultPhysicalPersistentStorageFactory.class);

    /**
     * Constructs a {@link PhysicalPersistentStore} from a {@link RocksDbPersistentStorageConfig}.
     *
     * @param config of the requested RocksDB persistent storage
     * @return RockDB implementation of {@link PhysicalPersistentStore}
     */
    public PhysicalPersistentStore constructPersistentStore(RocksDbPersistentStorageConfig config) {
        PersistentStorageFactories.sanitizeStoragePath(config.storagePath());
        File databaseFolder = new File(config.storagePath(), UUID.randomUUID().toString());
        RocksDB rocksDb = openRocksConnection(databaseFolder);
        return new RocksDbPhysicalPersistentStore(rocksDb, databaseFolder);
    }

    private static RocksDB openRocksConnection(File databaseFolder) {
        try {
            return RocksDB.open(databaseFolder.getAbsolutePath());
        } catch (RocksDBException e) {
            log.error("Opening RocksDB failed", e);
            throw new RuntimeException(e);
        }
    }
}
