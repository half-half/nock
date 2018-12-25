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

import com.half.nock.quartz.impl.IdempotentOperator;
import org.quartz.TriggerKey;
import org.quartz.impl.jdbcjobstore.FiredTriggerRecord;
import org.quartz.impl.jdbcjobstore.NoSuchDelegateException;
import org.quartz.impl.jdbcjobstore.StdJDBCDelegate;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.quartz.TriggerKey.triggerKey;


/**
 * Created by yuhuijuan on 2018/10/15
 * <p>
 * 信号量相关的操作，包括初始化，任务配对，分组,
 **/
public class SemaphoreStdJDBCDelegate extends StdJDBCDelegate implements NockJDBCConstants {
    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Data members.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    private TriggerManager triggerManager;

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Constructors.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * <p>
     * Create new SemaphoreStdJDBCDelegate instance.
     * </p>
     */
    public SemaphoreStdJDBCDelegate() {
    }

    /**
     * 初始化
     *
     * @param logger
     * @param tablePrefix
     * @param schedName
     * @param instanceId
     */
    public void initialize(Logger logger, String tablePrefix, String schedName, String instanceId, ClassLoadHelper classLoadHelper,
                           TriggerManager triggerManager) {
        this.triggerManager = triggerManager;
        try {
            super.initialize(logger, tablePrefix, schedName, instanceId, classLoadHelper, false, null);
        } catch (NoSuchDelegateException e) {
            e.printStackTrace();
        }

    }

    /**
     * 过滤不属于这个instance的trigger
     *
     * @param keys
     */
    private List<TriggerKey> triggerFilter(List<TriggerKey> keys) {
        List<TriggerKey> filterKeys = new ArrayList<>();

        if (keys != null && keys.size() > 0) {
            for (TriggerKey triggerKey : keys) {
                if (triggerManager.belongToCurrentNode(triggerKey)) {
                    filterKeys.add(triggerKey);
                }
            }
        }
        keys = null;
        return filterKeys;
    }

    /**
     * <p>
     * Select all of the triggers for jobs that are requesting recovery. The
     * returned trigger objects will have unique "recoverXXX" trigger names and
     * will be in the <code>{@link
     * org.quartz.Scheduler}.DEFAULT_RECOVERY_GROUP</code>
     * trigger group.
     * </p>
     * <p>
     * <p>
     * In order to preserve the ordering of the triggers, the fire time will be
     * set from the <code>COL_FIRED_TIME</code> column in the <code>TABLE_FIRED_TRIGGERS</code>
     * table. The caller is responsible for calling <code>computeFirstFireTime</code>
     * on each returned trigger. It is also up to the caller to insert the
     * returned triggers to ensure that they are fired.
     * </p>
     *
     * @param conn the DB Connection
     * @return an array of <code>{@link org.quartz.Trigger}</code> objects
     */
    public List<OperableTrigger> selectTriggersForRecoveringJobs(Connection conn, boolean isGrouped)
            throws SQLException, IOException, ClassNotFoundException {

        List<OperableTrigger> triggers = super.selectTriggersForRecoveringJobs(conn);
        if (isGrouped && (triggers != null && triggers.size() != 0)) {
            triggers.removeIf(x -> !(triggerManager.belongToCurrentNode(x.getKey())));
        }
        return triggers;
    }


    /**
     * <p>
     * Select the next trigger which will fire to fire between the two given timestamps
     * in ascending order of fire time, and then descending by priority.
     * </p>
     *
     * @param conn          the DB Connection
     * @param noLaterThan   highest value of <code>getNextFireTime()</code> of the triggers (exclusive)
     * @param noEarlierThan highest value of <code>getNextFireTime()</code> of the triggers (inclusive)
     * @param maxCount      maximum number of trigger keys allow to acquired in the returning list.
     * @return A (never null, possibly empty) list of the identifiers (Key objects) of the next triggers to be fired.
     */
    public List<TriggerKey> selectTriggerToAcquire(Connection conn, long noLaterThan, long noEarlierThan,
                                                   int maxCount, int batchSize, boolean isGrouped)
            throws SQLException {

        IdempotentOperator<List<TriggerKey>> idempotentOperator = new IdempotentOperator<List<TriggerKey>>(
                () -> {
                    List<TriggerKey> keys = selectTriggerToAcquire(conn, noLaterThan, noEarlierThan, isGrouped ? batchSize * maxCount : maxCount);
                    logger.debug(keys.toString());

                    List<TriggerKey> filteredKeys = null;

                    //if not acquiring any trigger
                    if (keys.size() == 0) {
                        return keys;
                    }

                    if (isGrouped) {
                        keys = triggerFilter(keys);
                        if (keys.size() > maxCount) {
                            filteredKeys = new ArrayList<>();
                            int i = 0;
                            while (i < maxCount) {
                                filteredKeys.add(keys.get(i));
                                i++;
                            }
                        }
                        logger.debug(keys.toString());

                        keys = filteredKeys == null ? keys : filteredKeys;
                        if (keys.size() == 0) {
                            throw new UnsupportedOperationException("didn't find useful triggers");
                        }

                    } else {
                        return keys;
                    }

                    return filteredKeys == null ? keys : filteredKeys;
                }

        );
        return idempotentOperator.execute(1000, 10L);

    }


    public List<FiredTriggerRecord> selectInstancesFiredTriggerRecords(Connection conn, String instanceName, boolean isGrouped) throws SQLException {
        List<FiredTriggerRecord> firedTriggerRecords = super.selectInstancesFiredTriggerRecords(conn, instanceName);
        if (isGrouped && (firedTriggerRecords != null && firedTriggerRecords.size() != 0)) {
            firedTriggerRecords.removeIf(x -> !(triggerManager.belongToCurrentNode(x.getTriggerKey())));
        }
        return firedTriggerRecords;
    }

    /**
     * <p>
     * Get the names of all of the triggers in the given state that have
     * misfired - according to the given timestamp.  No more than count will
     * be returned.
     * </p>
     *
     * @param conn       The DB Connection
     * @param count      The most misfired triggers to return, negative for all
     * @param resultList Output parameter.  A List of
     *                   <code>{@link org.quartz.utils.Key}</code> objects.  Must not be null.
     * @return Whether there are more misfired triggers left to find beyond
     * the given count.
     */
    public boolean hasMisfiredTriggersInState(Connection conn, String state1,
                                              long ts, int count, List<TriggerKey> resultList, boolean isGrouped) throws SQLException {
        boolean result = false;
        if (isGrouped) {
            result = hasMisfiredTriggersInStateInGrouped(conn, state1, ts, count, resultList);

        } else {
            result = super.hasMisfiredTriggersInState(conn, state1, ts, count, resultList);
        }
        return result;
    }


    /**
     * <p>
     * Get the number of triggers in the given states that have
     * misfired - according to the given timestamp.
     * </p>
     *
     * @param conn the DB Connection
     */
    public int countMisfiredTriggersInState(
            Connection conn, String state1, long ts, boolean isGrouped) throws SQLException {

        if (isGrouped) {
            return countMisfiredTriggersInStateInGrouped(conn, state1, ts);

        } else {
            return super.countMisfiredTriggersInState(conn, state1, ts);
        }

    }


    /**
     * <p>
     * Get the number of triggers in the given states that have
     * misfired - according to the given timestamp.
     * </p>
     *
     * @param conn the DB Connection
     */
    public int countMisfiredTriggersInStateInGrouped(Connection conn, String state1, long ts) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(rtp(COUNT_MISFIRED_TRIGGERS_IN_STATE_GROUPED));
            ps.setBigDecimal(1, new BigDecimal(String.valueOf(ts)));
            ps.setString(2, state1);
            rs = ps.executeQuery();

            List<TriggerKey> resultList = new ArrayList<>();
            while (rs.next()) {

                String triggerName = rs.getString(COL_TRIGGER_NAME);
                String groupName = rs.getString(COL_TRIGGER_GROUP);

                if (triggerManager.belongToCurrentNode((new TriggerKey(triggerName, groupName)))) {
                    resultList.add(triggerKey(triggerName, groupName));
                }
            }
            return resultList.size();
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    /**
     * <p>
     * Get the names of all of the triggers in the given state that have
     * misfired - according to the given timestamp.  No more than count will
     * be returned. filter unhandled triggers
     * </p>
     *
     * @param conn       The DB Connection
     * @param count      The most misfired triggers to return, negative for all
     * @param resultList Output parameter.  A List of
     *                   <code>{@link org.quartz.utils.Key}</code> objects.  Must not be null.
     * @return Whether there are more misfired triggers left to find beyond
     * the given count.
     */
    public boolean hasMisfiredTriggersInStateInGrouped(Connection conn, String state1,
                                                       long ts, int count, List<TriggerKey> resultList) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //恢复acqired状态的job
            ps = conn.prepareStatement(rtp(SELECT_HAS_MISFIRED_TRIGGERS_IN_STATE_ACQUAIRED));
            ps.setBigDecimal(1, new BigDecimal(String.valueOf(ts)));
            ps.setString(2, STATE_ACQUIRED);
            rs = ps.executeQuery();

            while (rs.next()) {
                String triggerName = rs.getString(COL_TRIGGER_NAME);
                String groupName = rs.getString(COL_TRIGGER_GROUP);
                if (triggerManager.belongToCurrentNode((new TriggerKey(triggerName, groupName)))) {
                    updateTriggerStateFromOtherState(conn, new TriggerKey(triggerName, groupName), STATE_WAITING, STATE_ACQUIRED);
                }
            }
            ps.close();
            ps = null;

            rs.close();
            rs = null;

            //查找misfired的 job
            ps = conn.prepareStatement(rtp(SELECT_HAS_MISFIRED_TRIGGERS_IN_STATE));
            ps.setBigDecimal(1, new BigDecimal(String.valueOf(ts)));
            ps.setString(2, state1);
            rs = ps.executeQuery();

            boolean hasReachedLimit = false;
            List<TriggerKey> testResultList = new ArrayList<>();
            while (rs.next() && (!hasReachedLimit)) {
                if (resultList.size() == count) {
                    hasReachedLimit = true;
                } else {
                    String triggerName = rs.getString(COL_TRIGGER_NAME);
                    String groupName = rs.getString(COL_TRIGGER_GROUP);

                    if (triggerManager.belongToCurrentNode((new TriggerKey(triggerName, groupName)))) {
                        // logger.info(">>>>>>>>>>>>>>>>>>>>>misfiredTriggers: triggerName: " + groupName + "." + triggerName + " nextFireTime:" + nextFireTime);
                        resultList.add(triggerKey(triggerName, groupName));
                    } else {
                        testResultList.add(triggerKey(triggerName, groupName));
                    }
                }
            }

            if (testResultList.size() > 0) {
                logger.debug(">>>>>>>>>>>>>>>>>>>>>>>>misfire triggers don't belong to this instance " + testResultList.toString());
            }

            return hasReachedLimit;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }

    /**
     * <p>
     * Update the given trigger to the given new state, if it is in the given
     * old state.
     * </p>
     *
     * @param conn     the DB connection
     * @param newState the new state for the trigger
     * @param oldState the old state the trigger must be in
     * @return int the number of rows updated
     * @throws SQLException
     */
    public int updateTriggerStateFromOtherStateWithTimestamp(Connection conn,
                                                                  TriggerKey triggerKey, String newState, String oldState, long nextFireTime) throws SQLException {
        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement(rtp(UPDATE_TRIGGER_STATE_FROM_STATE_WITH_TIMESTAMP));
            ps.setString(1, newState);
            ps.setString(2, triggerKey.getName());
            ps.setString(3, triggerKey.getGroup());
            ps.setString(4, oldState);
            ps.setBigDecimal(5, new BigDecimal(nextFireTime));

            return ps.executeUpdate();
        } finally {
            closeStatement(ps);
        }
    }


}

