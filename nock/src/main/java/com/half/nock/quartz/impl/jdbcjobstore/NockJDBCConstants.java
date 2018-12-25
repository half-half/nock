package com.half.nock.quartz.impl.jdbcjobstore;

import org.quartz.Trigger;
import org.quartz.impl.jdbcjobstore.StdJDBCConstants;

/**
 * Created by yuhuijuan on 2018/10/10
 */
public interface NockJDBCConstants extends StdJDBCConstants {
    String TABLE_SEMAPHORE_LOCKS = "SEMAPHORES";

    String COL_SEMAPHORE_NAME = "SEMAPHORE_NAME";

    String DEFAULT_SEMAPHORE_LOCK_PREFIX = "SEMAPHORE_";

    //sql for grouped triggers
    String SELECT_ALL_SEMAPHORE_LOCK = "SELECT * FROM "
            + TABLE_PREFIX_SUBST + TABLE_SEMAPHORE_LOCKS + " WHERE " + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST;

    String SELECT_FOR_SEMAPHORE_LOCK = "SELECT * FROM "
            + TABLE_PREFIX_SUBST + TABLE_SEMAPHORE_LOCKS + " WHERE " + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST
            + " AND " + COL_SEMAPHORE_NAME + " = ?";

    String INSERT_SEMAPHORE_LOCK = "INSERT INTO "
            + TABLE_PREFIX_SUBST + TABLE_SEMAPHORE_LOCKS + "(" + COL_SCHEDULER_NAME + ", " + COL_SEMAPHORE_NAME + " , " + COL_LAST_CHECKIN_TIME + ") VALUES ("
            + SCHED_NAME_SUBST + ", ? , 0)";

    String DELETE_ALL_SEMAPHORE_LOCK = "DELETE FROM " + TABLE_PREFIX_SUBST + TABLE_SEMAPHORE_LOCKS + " WHERE " + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST;

    String UPDATE_SEMAPHORE_INSTANCE = "UPDATE " + TABLE_PREFIX_SUBST + TABLE_SEMAPHORE_LOCKS
            + " SET " + COL_INSTANCE_NAME + " = ?," + COL_LAST_CHECKIN_TIME + " = ?"
            + " WHERE " + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST
            + " AND " + COL_SEMAPHORE_NAME + " = ?" + " AND "
            + COL_LAST_CHECKIN_TIME + " = ?";

    String SELECT_SEMAPHORE_FROM_INSTANCE = "SELECT * FROM " + TABLE_PREFIX_SUBST + TABLE_SEMAPHORE_LOCKS
            + " WHERE " + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST
            + " AND " + COL_INSTANCE_NAME + " = ?";

    String COUNT_MISFIRED_TRIGGERS_IN_STATE_GROUPED = "SELECT "
            + COL_TRIGGER_NAME + ", " + COL_TRIGGER_GROUP + " FROM "
            + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " WHERE "
            + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " AND NOT ("
            + COL_MISFIRE_INSTRUCTION + " = " + Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY + ") AND "
            + COL_NEXT_FIRE_TIME + " < ? "
            + "AND " + COL_TRIGGER_STATE + " = ?";


    String SELECT_HAS_MISFIRED_TRIGGERS_IN_STATE_ACQUAIRED = "SELECT "
            + COL_TRIGGER_NAME + ", " + COL_TRIGGER_GROUP + " FROM "
            + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " WHERE "
            + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " AND NOT ("
            + COL_MISFIRE_INSTRUCTION + " = " + Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY + ") AND "
            + COL_NEXT_FIRE_TIME + " < ? "
            + "AND " + COL_TRIGGER_STATE + " = ? "
            // AND TRIGGER_NAME NOT IN (SELECT  TRIGGER_NAME FROM qrtz_fired_triggers)
            + "AND " + COL_TRIGGER_NAME + " NOT IN ( SELECT " + COL_TRIGGER_NAME + " FROM " + TABLE_PREFIX_SUBST + TABLE_FIRED_TRIGGERS + " ) "
            + "ORDER BY " + COL_NEXT_FIRE_TIME + " ASC, " + COL_PRIORITY + " DESC";

    String UPDATE_TRIGGER_STATE_FROM_STATE_WITH_TIMESTAMP = "UPDATE "
            + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " SET " + COL_TRIGGER_STATE
            + " = ?" + " WHERE " + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST
            + " AND " + COL_TRIGGER_NAME + " = ? AND "
            + COL_TRIGGER_GROUP + " = ? AND " + COL_TRIGGER_STATE + " = ? AND " + COL_NEXT_FIRE_TIME + " = ? ";

}
