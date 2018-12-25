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
import org.quartz.impl.jdbcjobstore.DBSemaphore;
import org.quartz.impl.jdbcjobstore.LockException;
import org.quartz.impl.jdbcjobstore.Util;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Internal database based lock handler for providing thread/resource locking
 * in order to protect resources from being altered by multiple threads at the
 * same time.
 *
 * @author jhouse
 */

/**
 * Created by yuhuijuan on 2018/10/15
 * <p>
 * 根据配置项"org.quartz.jobStore.semaphoreCount:3"做判断，为空或者<1则保持原有操作，
 * >1则使用新的抢锁模式（乐观锁，任务分组）
 * </p>
 **/
public class StdRowLockSemaphore extends DBSemaphore implements NockJDBCConstants {

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Data members.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    protected static final String LOCK_STATE_ACCESS = "STATE_ACCESS";

    protected static final String LOCK_SEMAPHORE_ACCESS = "TRIGGER_SEMAPHORE_ACCESS";


    private int retryCount = 5;


    private List<TriggerSemaphore> semaphores;

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Constants.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    private int semaphoreCount = 1;

    private long clusterCheckinInterval = 7500l;

    private String instanceId;

    private String tablePrefix;
    private String schedName;

    private String semaphoreLockPrefix;

    private HashSet<String> locks = new HashSet<>();

    private ThreadLocal<String> semaphoreThreadLock = new ThreadLocal<>();

    private TriggerManager triggerManager;

    //default sql for un-grouped triggers
    public static final String SELECT_FOR_LOCK = "SELECT * FROM "
            + TABLE_PREFIX_SUBST + TABLE_LOCKS + " WHERE " + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST
            + " AND " + COL_LOCK_NAME + " = ? FOR UPDATE";

    public static final String INSERT_LOCK = "INSERT INTO "
            + TABLE_PREFIX_SUBST + TABLE_LOCKS + "(" + COL_SCHEDULER_NAME + ", " + COL_LOCK_NAME + ") VALUES ("
            + SCHED_NAME_SUBST + ", ?)";

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Constructors.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    public StdRowLockSemaphore(String tablePrefix, String schedName, String selectWithLockSQL, int semaphoreCount, String instanceId,
                               String semaphoreLockPrefix, TriggerManager triggerManager, long clusterCheckinInterval) {
        super(tablePrefix, schedName, selectWithLockSQL != null ? selectWithLockSQL : semaphoreCount > 1 ? SELECT_FOR_SEMAPHORE_LOCK : SELECT_FOR_LOCK, semaphoreCount > 1 ? INSERT_SEMAPHORE_LOCK : INSERT_LOCK);
        this.instanceId = instanceId;
        this.tablePrefix = tablePrefix;
        this.schedName = schedName;
        this.semaphoreCount = semaphoreCount < 1 ? 1 : semaphoreCount;
        this.semaphoreLockPrefix = semaphoreLockPrefix == null ? DEFAULT_SEMAPHORE_LOCK_PREFIX : semaphoreLockPrefix;
        this.triggerManager = triggerManager;
        this.clusterCheckinInterval = clusterCheckinInterval;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * get semaphoreList of semaphore table
     *
     * @return semaphoreList
     */
    public List<TriggerSemaphore> getSemaphores() {
        return semaphores;
    }
    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Interface.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * Execute the SQL select for update that will lock the proper database row.
     */
    @Override
    protected void executeSQL(Connection conn, final String lockName, String expandedSQL, String expandedInsertSQL) throws LockException {
        SQLException initCause = null;

        // attempt lock two times (to work-around possible race conditions in inserting the lock row the first time running)
        int count = 0;
        do {
            count++;
            try {

                if (semaphoreCount <= 1 || !lockName.startsWith(semaphoreLockPrefix)) {
                    expandedSQL = rtp(SELECT_FOR_LOCK);
                    expandedInsertSQL = rtp(INSERT_LOCK);
                    executeInLocks(conn, lockName, expandedSQL, expandedInsertSQL, count);
                } else {
                    expandedSQL = rtp(SELECT_FOR_SEMAPHORE_LOCK);
                    executeInTriggerSemaphores(conn, lockName, expandedSQL);
                }
                return; // obtained lock, go
            } catch (SQLException sqle) {
                //Exception src =
                // (Exception)getThreadLocksObtainer().get(lockName);
                //if(src != null)
                //  src.printStackTrace();
                //else
                //  System.err.println("--- ***************** NO OBTAINER!");

                if (initCause == null)
                    initCause = sqle;

                if (getLog().isDebugEnabled()) {
                    getLog().debug(
                            "Lock '" + lockName + "' was not obtained by: " +
                                    Thread.currentThread().getName() + (count < 3 ? " - will try again." : ""));
                }

                if (count < 3) {
                    // pause a bit to give another thread some time to commit the insert of the new lock row
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException ignore) {
                        Thread.currentThread().interrupt();
                    }
                    // try again ...
                    continue;
                }

                throw new LockException("Failure obtaining db row lock: "
                        + sqle.getMessage(), sqle);
            }
        } while (count < 4);

        throw new LockException("Failure obtaining db row lock, reached maximum number of attempts. Initial exception (if any) attached as root cause.", initCause);
    }

    protected String getSelectWithLockSQL() {
        return getSQL();
    }

    public void setSelectWithLockSQL(String selectWithLockSQL) {
        setSQL(selectWithLockSQL);
    }

    /**
     * retain the original action,get locks from LOCKS TABLE
     *
     * @param conn
     * @param lockName
     * @param expandedSQL
     * @param expandedInsertSQL
     * @param count             retry count
     * @throws SQLException
     */
    private void executeInLocks(Connection conn, final String lockName, final String expandedSQL, final String expandedInsertSQL, int count) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            ps = conn.prepareStatement(expandedSQL);
            ps.setString(1, lockName);

            if (getLog().isDebugEnabled()) {
                getLog().debug(
                        "Lock '" + lockName + "' is being obtained: " +
                                Thread.currentThread().getName());
            }
            rs = ps.executeQuery();
            if (!rs.next()) {
                getLog().debug(
                        "Inserting new lock row for lock: '" + lockName + "' being obtained by thread: " +
                                Thread.currentThread().getName());
                rs.close();
                rs = null;
                ps.close();
                ps = null;
                ps = conn.prepareStatement(expandedInsertSQL);
                ps.setString(1, lockName);

                int res = ps.executeUpdate();

                if (res != 1) {
                    if (count < 3) {
                        // pause a bit to give another thread some time to commit the insert of the new lock row
                        try {
                            Thread.sleep(1000L);
                        } catch (InterruptedException ignore) {
                            Thread.currentThread().interrupt();
                        }
                        // try again ...
                        // continue;
                        executeInLocks(conn, lockName, expandedSQL, expandedInsertSQL, count + 1);
                    }

                    throw new SQLException(Util.rtp(
                            "No row exists, and one could not be inserted in table " + TABLE_PREFIX_SUBST + TABLE_LOCKS +
                                    " for lock named: " + lockName, getTablePrefix(), getSchedulerNameLiteral()));
                }
            }
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
        }
    }

    /**
     * 1.查找对应的semaphore
     * 2.查不到：insert，return step 1
     * 查到：继续
     * 3.根据last_checkin_time做乐观锁
     *
     * @param conn
     * @param semaphoreName semaphore name
     * @param expandedSQL
     * @throws SQLException
     */
    private void executeInTriggerSemaphores(Connection conn, String semaphoreName, final String expandedSQL) throws SQLException {
        boolean getLock = false;
        PreparedStatement ps = null;
        ResultSet rs = null;
        while (!getLock) {
            try {
                ps = conn.prepareStatement(expandedSQL);
                ps.setString(1, semaphoreName);

                rs = ps.executeQuery();
                if (rs.next()) {
                    //乐观锁获取

                    String instance_lock = rs.getString(COL_INSTANCE_NAME);
                    long obtainedTime = rs.getLong(COL_LAST_CHECKIN_TIME);

                    //如果乐观锁没人占领,obtain lock for this instance
                    if (instance_lock == null || instance_lock.equals("")) {
                        long newCheckinTime = System.currentTimeMillis();
                        getLog().info("trying to obtain DBlock...: " + semaphoreName);
                        getLock = updateSemaphoreWithInstance(conn, semaphoreName, instanceId, rs.getLong(COL_LAST_CHECKIN_TIME), newCheckinTime) == 1;

                        //if obtain success,try obtain thread lock
                        if (getLock) {
                            getLog().info("trying to obtain thread lock...: " + semaphoreName);
                            getLock = obtainSemaphoreLock(semaphoreName);
                        }
                        getLog().info("thread lock and DB lock have been obtained...: " + semaphoreName);

                    } else if (instance_lock.equals(instanceId)) {
                        //if semaphore lock has been obtained by this instance,check thread locks
                        getLock = obtainSemaphoreLock(semaphoreName);
                    } else {
                        if (releaseExpiredSemaphoreLock(conn, instance_lock, semaphoreName, obtainedTime)) {
                            continue;
                        }
                        //if semaphore lock has been obtained by others,sleep
                        try {
                            Thread.sleep(10000L);
                            semaphoreName = getSemaphoreLockForInstance(conn);
                            getLog().debug("finishing waiting for lock:" + semaphoreName);

                        } catch (InterruptedException | JobPersistenceException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } finally {

                closeStatement(ps);
                closeResultSet(rs);
            }
        }

    }

    /**
     * 更新semaphore表的乐观锁，检查last_checkin_time是否一致
     *
     * @param conn
     * @param lastCheckinTime
     * @return
     * @throws SQLException
     */
    public int updateSemaphoreWithInstance(Connection conn, String semaphoreName, String instanceId, Long lastCheckinTime, Long newCheckinTime) throws SQLException {
        PreparedStatement ps = null;
        int rs;
        try {
            ps = conn.prepareStatement(rtp(UPDATE_SEMAPHORE_INSTANCE));
            ps.setString(1, instanceId);
            ps.setBigDecimal(2, new BigDecimal(String.valueOf(newCheckinTime)));
            ps.setString(3, semaphoreName);
            ps.setBigDecimal(4, new BigDecimal(String.valueOf(lastCheckinTime)));

            rs = ps.executeUpdate();

            return rs;

        } finally {
            closeStatement(ps);
        }
    }

    /**
     * if this instance obtains wrong semaphore lock,release it,which often happens when initializing semaphores or
     * schedulers is going online or offline.
     *
     * @param conn
     */
    public void releaseWrongSemaphoreLock(Connection conn) {

        PreparedStatement ps = null;
        try {
            String currentSemaphore = getSemaphoreLockForInstance(conn);

            if (semaphores == null || semaphores.size() == 0) {
                return;
            }
            for (TriggerSemaphore triggerSemaphore : semaphores) {

                if (!triggerSemaphore.getName().equals(currentSemaphore) && (triggerSemaphore.getInstanceName() != null && triggerSemaphore.getInstanceName().equals(instanceId))) {
                    //释放锁
                    ps = conn.prepareStatement(rtp(UPDATE_SEMAPHORE_INSTANCE));
                    ps.setString(1, null);
                    ps.setBigDecimal(2, new BigDecimal(String.valueOf(System.currentTimeMillis())));
                    ps.setString(3, triggerSemaphore.getName());
                    ps.setBigDecimal(4, new BigDecimal(String.valueOf(triggerSemaphore.getCheckTime())));

                    int res = ps.executeUpdate();

                    getLog().debug("current semaphore is : " + currentSemaphore + "," + Thread.currentThread().getName() + " release db lock " + triggerSemaphore.getName());
                    ps.close();
                    ps = null;

                }
            }

        } catch (JobPersistenceException | SQLException e) {
            e.printStackTrace();
        } finally {
            closeStatement(ps);
        }
    }

    /**
     * if this semaphore lock has been obtained by a offline instance, release it.
     *
     * @param conn
     * @param instanceLock
     */
    public boolean releaseExpiredSemaphoreLock(Connection conn, String instanceLock, String lockName, long obtainedTime) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        long lastCheckInTime = 0;

        try {
            ps = conn.prepareStatement(rtp(SELECT_SCHEDULER_STATE));
            ps.setString(1, instanceLock);
            rs = ps.executeQuery();
            if (rs.next()) {
                lastCheckInTime = rs.getLong(COL_LAST_CHECKIN_TIME);

                if (System.currentTimeMillis() - lastCheckInTime <= clusterCheckinInterval) {
                    return false;
                }
            }

        } finally {
            closeStatement(ps);
            closeResultSet(rs);
        }
        int res = updateSemaphoreWithInstance(conn, lockName, null, obtainedTime, System.currentTimeMillis());
        return res > 0;
    }

    /**
     * 将锁存在hashSet中，保证线程级别的锁
     *
     * @param lockName
     */
    private synchronized boolean obtainSemaphoreLock(String lockName) {

        while (locks.contains(lockName)) {
            try {
                getLog().debug(">>> waiting for thread lock......" + lockName);
                this.wait();
                getLog().debug(">>> obtain for thread lock again......" + lockName);

            } catch (InterruptedException e) {
                getLog().debug(
                        "Lock '" + lockName + "' was not obtained by: "
                                + Thread.currentThread().getName());
            }
        }
        locks.add(lockName);
        semaphoreThreadLock.set(lockName);
        getLog().debug("has obtained thread lock " + lockName);

        return true;
    }

    /**
     * release thread lock
     */
    public synchronized void releaseSemaphoreLock() {
        locks.remove(semaphoreThreadLock.get());
        this.notifyAll();
        // getLog().info(">>> release thread lock " + lockName);

    }

    /**
     * <p>
     * Replace the table prefix in a query by replacing any occurrences of
     * "{0}" with the table prefix.
     * </p>
     *
     * @param query the unsubstitued query
     * @return the query, with proper table prefix substituted
     */
    protected final String rtp(String query) {
        return Util.rtp(query, tablePrefix, getSchedulerNameLiteral());
    }

    /**
     * 1.select semaphore table,查询信号量的数量是否匹配（无锁）
     * 2. -if wrong , lock LOCKS TABLE ,double check, insert semaphore,initialize hash circle
     * -if right ,pass
     * 3.select STATE TABLE ,寻找匹配的semaphore
     *
     * @return
     */
    public String getSemaphoreLockForInstance(Connection conn) throws SQLException, JobPersistenceException {

        String currentLockName;
        List<TriggerSemaphore> currentSemaphores = getSemaphoresFromDB(conn);

        int retry = retryCount;


        //尝试5次完成初始化semaphores table
        while (retry-- > 0 && ifSemaphoreChanged(currentSemaphores)) {
            initializeSemaphore(conn);
            currentSemaphores = getSemaphoresFromDB(conn);
        }

        if (currentSemaphores == null || currentSemaphores.size() != semaphoreCount) {
            throw new JobPersistenceException("can't get right semaphore!");
        }
        //初始化consistant hash
        if (semaphores == null || !semaphoreEquals(semaphores, currentSemaphores)) {
            triggerManager.reset();
            currentSemaphores.forEach(x -> triggerManager.addInstanceNode(x.getName()));
        }
        semaphores = currentSemaphores;

        int index = -1;
        List<String> schedulerList = null;
        //防止clusermanager还没有报心跳
        while (index == -1) {
            schedulerList = getSchedulerFromDB(conn);
            index = schedulerList.indexOf(instanceId);
            if (index == -1) {
                getLog().warn("waiting for clusterManager.....");
                clusterCheckIn(conn);
            }
        }

        try {
            currentLockName = semaphores.get(index % semaphoreCount).getName();
        } catch (ArrayIndexOutOfBoundsException e) {
            currentLockName = semaphores.get(0).getName();
        }
        triggerManager.setCurrentSemaphoreLock(currentLockName);
        return currentLockName;
    }

    /**
     * 检查数据库的trigger semaphores是否发生改变
     *
     * @param currentSemaphores
     * @return
     */
    private boolean ifSemaphoreChanged(List<TriggerSemaphore> currentSemaphores) {
        if ((currentSemaphores == null || currentSemaphores.size() != semaphoreCount)) {
            return true;
        } else if (currentSemaphores.size() > 0) {
            //check if semaphoreLockPrefix has changed
            for (TriggerSemaphore tr : currentSemaphores) {
                if (!tr.getName().startsWith(semaphoreLockPrefix)) {
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * 获取所有的semaphores
     *
     * @param conn
     * @return
     * @throws SQLException
     */
    public List<TriggerSemaphore> getSemaphoresFromDB(Connection conn) throws SQLException {
        List<TriggerSemaphore> triggerSemaphores = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(rtp(SELECT_ALL_SEMAPHORE_LOCK));
            rs = ps.executeQuery();

            while (rs.next()) {
                TriggerSemaphore triggerSemaphore = new TriggerSemaphore();
                triggerSemaphore.setName(rs.getString(NockJDBCConstants.COL_SEMAPHORE_NAME));
                triggerSemaphore.setInstanceName(rs.getString(COL_INSTANCE_NAME));
                triggerSemaphore.setCheckTime(rs.getLong(COL_LAST_CHECKIN_TIME));
                triggerSemaphores.add(triggerSemaphore);
            }
            if (getLog().isDebugEnabled()) {
                getLog().debug(
                        "select " + triggerSemaphores.size() + " semaphores from db");
            }
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }

        return triggerSemaphores;
    }

    /**
     * 从DB获取所有的调度实例
     * todo:是否需要LOCK
     *
     * @param conn
     * @return
     * @throws SQLException
     */
    public List<String> getSchedulerFromDB(Connection conn) throws SQLException {
        List<String> schedulers = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(rtp(SELECT_SCHEDULER_STATES));
            rs = ps.executeQuery();

            while (rs.next()) {
                schedulers.add(rs.getString(COL_INSTANCE_NAME));
            }
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }

        return schedulers;
    }

    /**
     * 初始化trigger senmaphores
     *
     * @param conn
     * @throws LockException
     * @throws SQLException
     */
    private void initializeSemaphore(Connection conn) throws JobPersistenceException, SQLException {

        conn.setAutoCommit(false);

        obtainLock(conn, LOCK_SEMAPHORE_ACCESS);
        PreparedStatement ps = null;
        int rs = 0;

        //double check
        List<TriggerSemaphore> triggerSemaphoreList = getSemaphoresFromDB(conn);
        try {
            if (triggerSemaphoreList.size() != semaphoreCount) {
                if (triggerSemaphoreList.size() > 0) {
                    ps = conn.prepareStatement(rtp(DELETE_ALL_SEMAPHORE_LOCK));
                    rs = ps.executeUpdate();
                    if (rs == 0) {
                        throw new SQLException("DELETE SEMAPHORE FAIL:" + schedName);
                    }
                }
                int currentCount = 0;

                while (currentCount++ < semaphoreCount) {
                    ps = conn.prepareStatement(rtp(INSERT_SEMAPHORE_LOCK));
                    String semaphoreName = semaphoreLockPrefix + System.currentTimeMillis();
                    ps.setString(1, semaphoreName);
                    rs = ps.executeUpdate();
                    if (rs != 1) {
                        throw new SQLException("INSERT SEMAPHORE FAIL");
                    }
                }

            }
            try {
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw new JobPersistenceException(
                        "Couldn't commit jdbc connection. " + e.getMessage(), e);
            }
        } finally {
            conn.setAutoCommit(true);
            releaseLock(LOCK_SEMAPHORE_ACCESS);
            closeStatement(ps);
        }

    }

    /**
     * get semaphore locks which the instance obtains
     *
     * @param conn
     * @param instanceId
     * @return
     */
    public List<TriggerSemaphore> getSemaphoresByInstances(Connection conn, String instanceId) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        List<TriggerSemaphore> semaphoreList = new ArrayList<>();

        try {
            ps = conn.prepareStatement(rtp(SELECT_SEMAPHORE_FROM_INSTANCE));
            ps.setString(1, instanceId);
            rs = ps.executeQuery();
            while (rs.next()) {
                TriggerSemaphore semaphore = new TriggerSemaphore();
                semaphore.setName(rs.getString(NockJDBCConstants.COL_SEMAPHORE_NAME));
                semaphore.setInstanceName(rs.getString(COL_INSTANCE_NAME));
                semaphore.setCheckTime(rs.getLong(COL_LAST_CHECKIN_TIME));
                semaphoreList.add(semaphore);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeResultSet(rs);
            closeStatement(ps);

        }
        return semaphoreList;
    }

    private void clusterCheckIn(Connection conn)
            throws JobPersistenceException {
        try {
            conn.setAutoCommit(false);
            obtainLock(conn, LOCK_STATE_ACCESS);
            List<String> schedulerList = getSchedulerFromDB(conn);
            int index = schedulerList.indexOf(instanceId);
            if (index == -1) {
                // check in...
                long lastCheckin = System.currentTimeMillis();
                if (updateSchedulerState(conn, instanceId, lastCheckin) == 0) {
                    insertSchedulerState(conn, instanceId, lastCheckin, clusterCheckinInterval);
                }
            }
            try {
                conn.commit();
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                conn.rollback();
                throw new JobPersistenceException(
                        "Couldn't commit jdbc connection. " + e.getMessage(), e);
            }
        } catch (Exception e) {
            throw new JobPersistenceException("Failure inserting scheduler state when checking-in: "
                    + e.getMessage(), e);
        } finally {
            releaseLock(LOCK_STATE_ACCESS);
        }

    }

    private int insertSchedulerState(Connection conn, String theInstanceId, long checkInTime, long interval)
            throws SQLException {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(rtp(INSERT_SCHEDULER_STATE));
            ps.setString(1, theInstanceId);
            ps.setLong(2, checkInTime);
            ps.setLong(3, interval);

            return ps.executeUpdate();
        } finally {
            closeStatement(ps);
        }
    }

    private int updateSchedulerState(Connection conn, String theInstanceId, long checkInTime)
            throws SQLException {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(rtp(UPDATE_SCHEDULER_STATE));
            ps.setLong(1, checkInTime);
            ps.setString(2, theInstanceId);

            return ps.executeUpdate();
        } finally {
            closeStatement(ps);
        }
    }

    /**
     * 比较semaphore集合是否相同
     *
     * @param oldSemaphoreList
     * @param newSemaphoreList
     * @return
     */
    private boolean semaphoreEquals
    (List<TriggerSemaphore> oldSemaphoreList, List<TriggerSemaphore> newSemaphoreList) {
        if (oldSemaphoreList.getClass() != newSemaphoreList.getClass()) {
            return false;
        }

        if (oldSemaphoreList.size() != newSemaphoreList.size()) {
            return false;
        }
        for (int i = 0; i < oldSemaphoreList.size(); i++) {
            if (!oldSemaphoreList.get(i).getName().equals(newSemaphoreList.get(i).getName())) {
                return false;
            }
        }
        return true;
    }


    /**
     * Cleanup helper method that closes the given <code>ResultSet</code>
     * while ignoring any errors.
     */
    protected static void closeResultSet(ResultSet rs) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException ignore) {
            }
        }
    }

    /**
     * Cleanup helper method that closes the given <code>Statement</code>
     * while ignoring any errors.
     */
    protected static void closeStatement(Statement statement) {
        if (null != statement) {
            try {
                statement.close();
            } catch (SQLException ignore) {
            }
        }
    }

}
