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
