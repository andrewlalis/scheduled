# scheduled
Simple CRON-style scheduling library for D projects.

## Usage
The following is a barebones example for how to use **scheduled**.
```d
import std.stdio;
import core.time;
import std.datetime;

import scheduled;

void main() {
	JobScheduler scheduler = new ThreadedJobScheduler;
	scheduler.addJob(
		() => writeln("Executing at " ~ Clock.currTime.toISOExtString),
		new FixedIntervalSchedule(seconds(12))
	);
	scheduler.start();
}
```

In addition to the `FixedIntervalSchedule`, the following is a list of all supported schedules:
- `OneTimeSchedule` - Executes a job once, at a specified timestamp.
- `FixedIntervalSchedule` - Executes a job repeatedly, according to a specified `Duration` interval.
- `DailySchedule` - Executes a job one or more times per day, at specified times.
- `CronSchedule` - Executes a job according to a [Cron expression](https://en.wikipedia.org/wiki/Cron). For a precise overview of the supported cron syntax, please refer to Maxim Tyapkin's [`cronexp` library](https://cronexp.dpldocs.info/source/cronexp.cronexp.d.html).

By default, the library comes with the following `JobScheduler` implementations:
- `ThreadedJobScheduler` - Scheduler which runs as a thread, and uses a [TaskPool](https://dlang.org/library/std/parallelism/task_pool.html) to execute jobs. It keeps all scheduled jobs in a priority queue, and sleeps until the nearest job should be executed.
