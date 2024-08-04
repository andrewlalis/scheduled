module scheduled.schedules.cron_schedule;

import std.typecons;
import std.datetime;

import scheduled.cronexp : CronExpr;
import scheduled.schedule;

/** 
 * The Cron schedule makes use of a CRON-style expression string to define the
 * times at which jobs will be run. Note that Cron expressions have no concept
 * of time zones, so all times are interpreted as in the local time zone.
 * 
 * For more information on the specific format, please see https://github.com/Kegian/cron
 */
public class CronSchedule : JobSchedule {
    private CronExpr cronExpression;

    public this(CronExpr cronExpression) {
        this.cronExpression = cronExpression;
    }

    public this(string cronExpressionString) {
        this(CronExpr(cronExpressionString));
    }

    Nullable!SysTime getNextExecutionTime(SysTime currentTime) const {
        DateTime currentDateTime = cast(DateTime)currentTime.toLocalTime;
        Nullable!DateTime dateTime = this.cronExpression.getNext(currentDateTime);
        if (dateTime.isNull) {
            return Nullable!SysTime.init;
        } else {
            DateTime d = dateTime.get;
            return SysTime(d).nullable;
        }
    }

    void markExecuted(SysTime executionTime) {
        // Don't do anything.
    }

    bool isRepeating() const {
        return true;
    }
}
