/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.half.nock.quartz.impl.jdbcjobstore;

import org.quartz.JobPersistenceException;
import org.quartz.SchedulerConfigException;
import org.quartz.impl.jdbcjobstore.JobStoreCMT;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerSignaler;

import java.sql.Connection;

/**
 * <p>
 * <code>JobStoreTX</code> is meant to be used in a standalone environment.
 * Both commit and rollback will be handled by this class.
 * </p>
 * <p>
 * <p>
 * If you need a <code>{@link org.quartz.spi.JobStore}</code> class to use
 * within an application-server environment, use <code>{@link
 * JobStoreCMT}</code>
 * instead.
 * </p>
 *
 * @author <a href="mailto:jeff@binaryfeed.org">Jeffrey Wescott</a>
 * @author James House
 */
public class JobStoreTX extends JobStoreSupport {
    /**
     * Execute the given callback having optionally acquired the given lock.
     * This uses the non-managed transaction connection.
     * <p>
     * get lock form semaphores table like "semaphore-113446745757"
     */
    @Override
    protected <T> T executeInNonManagedTXLock(
            String lockName, TransactionCallback<T> txCallback, TransactionValidator<T> txValidator) throws JobPersistenceException {
        boolean transOwner = false;
        Connection conn = null;
        try {
            if (lockName != null) {
                // If we aren't using db locks, then delay getting DB connection
                // until after acquiring the lock since it isn't needed.
                if (getLockHandler().requiresConnection()) {
                    conn = getNonManagedTXConnection();
                }

                transOwner = getLockHandler().obtainLock(conn, lockName);
            }

            if (conn == null) {
                conn = getNonManagedTXConnection();
            }

            final T result = txCallback.execute(conn);
            try {
                commitConnection(conn);
            } catch (JobPersistenceException e) {
                rollbackConnection(conn);
                if (txValidator == null || !retryExecuteInNonManagedTXLock(lockName,
                        new TransactionCallback<Boolean>() {
                            @Override
                            public Boolean execute(Connection conn) throws JobPersistenceException {
                                return txValidator.validate(conn, result);
                            }
                        })) {
                    throw e;
                }
            }

            Long sigTime = clearAndGetSignalSchedulingChangeOnTxCompletion();
            if (sigTime != null && sigTime >= 0) {
                signalSchedulingChangeImmediately(sigTime);
            }

            return result;
        } catch (JobPersistenceException e) {
            rollbackConnection(conn);
            throw e;
        } catch (RuntimeException e) {
            rollbackConnection(conn);
            throw new JobPersistenceException("Unexpected runtime exception: "
                    + e.getMessage(), e);
        } finally {
            try {
                releaseLock(conn, lockName, transOwner);
            } finally {
                cleanupConnection(conn);
            }
        }
    }

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * copy all from JobStoreTX
     *
     * Interface.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */


    @Override
    public void initialize(ClassLoadHelper classLoadHelper,
                           SchedulerSignaler schedSignaler) throws SchedulerConfigException {

        super.initialize(classLoadHelper, schedSignaler);

        getLog().info("JobStoreTX initialized.");
    }

    /**
     * For <code>JobStoreTX</code>, the non-managed TX connection is just
     * the normal connection because it is not CMT.
     *
     * @see JobStoreSupport#getConnection()
     */
    @Override
    protected Connection getNonManagedTXConnection()
            throws JobPersistenceException {
        return getConnection();
    }

    /**
     * Execute the given callback having optionally aquired the given lock.
     * For <code>JobStoreTX</code>, because it manages its own transactions
     * and only has the one datasource, this is the same behavior as
     * executeInNonManagedTXLock().
     *
     * @param lockName The name of the lock to aquire, for example
     *                 "TRIGGER_ACCESS".  If null, then no lock is aquired, but the
     *                 lockCallback is still executed in a transaction.
     * @see JobStoreSupport#getNonManagedTXConnection()
     * @see JobStoreSupport#getConnection()
     */
    @Override
    protected Object executeInLock(
            String lockName,
            TransactionCallback txCallback) throws JobPersistenceException {
        return executeInNonManagedTXLock(lockName, txCallback, null);
    }
}
// EOF
