import std.stdio;
import core.time;
import std.parallelism;
import std.datetime;

import scheduled;

class SimpleJob : Job {
	private const string name;
	
	this(string name) {
		this.name = name;
	}

	override void run() {
		auto st = Clock.currTime;
		writeln("Executing task " ~ this.name ~ " at " ~ st.toISOExtString);
	}
}

void main() {
	JobScheduler scheduler = new ThreadedJobScheduler;
	scheduler.addJob(new SimpleJob("first"), new FixedIntervalSchedule(seconds(3)));
	scheduler.addJob(new SimpleJob("second"), new FixedIntervalSchedule(seconds(10)));
	scheduler.start();
}
