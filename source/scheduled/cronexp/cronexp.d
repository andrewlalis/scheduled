/**
  * This framework provides parser of cron expressions and evaluator
  * of next date and time of triggering parsed cron expressions.
  *
  * Cron expression consists of 6 required fields separated by white space
  *
  * Supported fields:
  *
  * Field           Allowed values      Special charachters
  *
  * Seconds         0-59                , - * /
  * Minutes         0-59                , - * /
  * Hours           0-23                , - * /
  * Day-of-month    1-31                , - * / ?
  * Month           1-12 or JAN-DEC     , - * /
  * Day-of-week     1-7 or MON-SUN      , - * / ?
  *
  * Months names: JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC
  * Day-of-weeks names: MON,TUE,WED,THU,FRI,SAT,SUN
  *
  * Charachters:
  *
  * Asterisk (*):
  *     is used for specify all values (eg. 'every minute' in minute field)
  *
  * Comma (,):
  *     is used for to separate items of a list (eg. using "MON,WED,FRI"
  *     in  the 6th field (day of week) means Mondays, Wednesdays, Fridays)
  *
  * Hyphen (-):
  *     is used for define ranges (eg. 10-30 in 1st field means every second
  *     between 10 and 30 inclusive)
  *
  * Slash (/):
  *     is used for define step values, a/x = a, a+x, a+2x, a+3x, ...
  *     (eg. 2/4 in hours means list of 2,6,10,14,18,22)
  *     '*' mean minimal allowed value (eg. 0 for hours, 1 for months etc)
  *
  * Question mark (?):
  *     is a synonym of '*' for day-of-week and day-of-month fields used
  *     to explicitly indicate that days are set with other field
  *
  *
  * NOTES:
  *     - If both the 'Day-of-month' and 'Day-of-week' fields are
  *       restricted (aren't '*'), next correct time will be when both(!)
  *       fields match the conditions.
  *       For example: `* * * 13 * FRI` expression will be satisfied only
  *       on friday 13th.
  *     - Ranges can be overflowing, it means range 'NOV-FEB' (NOV > FEB)
  *       will expand in NOV,DEC,JAN,FEB
  *
  *
  *
  * Copyright:
  *     Copyright (c) 2018, Maxim Tyapkin.
  * Authors:
  *     Maxim Tyapkin
  * License:
  *     This software is licensed under the terms of the BSD 3-clause license.
  *     The full terms of the license can be found in the LICENSE.md file.
  */

module scheduled.cronexp.cronexp;


private
{
    import std.algorithm : filter, each, map;
    import std.array : array, split;
    import std.conv : to;
    import std.datetime;
    import std.range : iota;
    import std.regex : ctRegex, matchFirst, Captures;
    import std.traits : isSomeString, EnumMembers;
    import std.typecons : tuple, Tuple, Nullable;

    import scheduled.cronexp.utils;
}



alias Interval = Tuple!(ubyte, "from", ubyte, "to");

enum Range : Interval
{
    sec = Interval(0, 59),
    min = Interval(0, 59),
    hrs = Interval(0, 23),
    dom = Interval(1, 31),
    mon = Interval(1, 12),
    dow = Interval(1, 7)
}


mixin template RegexNamesEnum(string Name, Fields...)
{
    private static string generate()
    {
        auto result = "enum " ~ Name ~ " : string {";

        foreach (field; Fields)
            result ~= field ~ " = r\"" ~ Name ~ field ~ "\",";

        return result ~ "}";
    }

    mixin(generate());
}


mixin RegexNamesEnum!("Sec", "list", "range", "seq", "any");
mixin RegexNamesEnum!("Min", "list", "range", "seq", "any");
mixin RegexNamesEnum!("Hrs", "list", "range", "seq", "any");
mixin RegexNamesEnum!("Dom", "list", "range", "seq", "undef", "any");
mixin RegexNamesEnum!("Mon", "list", "range", "seq", "listN", "rangeN", "seqN", "any");
mixin RegexNamesEnum!("Dow", "list", "range", "seq", "listN", "rangeN", "seqN", "undef", "any");


enum moyNames = r"JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC";
enum dowNames = r"MON|TUE|WED|THU|FRI|SAT|SUN";

enum secReg = r"(?P<"~Sec.list~r">(([0-9]|[0-5][0-9]),)*([0-9]|[0-5][0-9]))" ~ r"|" ~ 
              r"(?P<"~Sec.range~r">([0-9]|[0-5][0-9])-([0-9]|[0-5][0-9]))" ~ r"|" ~ 
              r"(?P<"~Sec.seq~r">([\*]|[0-9]|[0-5][0-9])\/([0-9]|[0-5][0-9]))" ~ r"|" ~ 
              r"(?P<"~Sec.any~r">[\*])";

enum minReg = r"(?P<"~Min.list~r">(([0-9]|[0-5][0-9]),)*([0-9]|[0-5][0-9]))" ~ r"|" ~ 
              r"(?P<"~Min.range~r">([0-9]|[0-5][0-9])-([0-9]|[0-5][0-9]))" ~ r"|" ~ 
              r"(?P<"~Min.seq~r">([\*]|[0-9]|[0-5][0-9])\/([0-9]|[0-5][0-9]))" ~ r"|" ~ 
              r"(?P<"~Min.any~r">[\*])";

enum hrsReg = r"(?P<"~Hrs.list~r">(([0-9]|[0-1][0-9]|[2][0-3]),)*([0-9]|[0-1][0-9]|[2][0-3]))" ~ r"|" ~
              r"(?P<"~Hrs.range~r">([0-9]|[0-1][0-9]|[2][0-3])-([0-9]|[0-1][0-9]|[2][0-3]))" ~ r"|" ~
              r"(?P<"~Hrs.seq~r">([\*]|[0-9]|[0-1][0-9]|[2][0-3])\/([0-9]|[0-1][0-9]|[2][0-3]))" ~ r"|" ~
              r"(?P<"~Hrs.any~r">[\*])";

enum domReg = r"(?P<"~Dom.list~r">(([1-9]|[0][1-9]|[1-2][0-9]|[3][0-1]),)*([1-9]|[0][1-9]|[1-2][0-9]|[3][0-1]))" ~ r"|" ~
              r"(?P<"~Dom.range~r">([1-9]|[0][1-9]|[1-2][0-9]|[3][0-1])-([1-9]|[0][1-9]|[1-2][0-9]|[3][0-1]))" ~ r"|" ~
              r"(?P<"~Dom.seq~r">([\*]|[1-9]|[0][1-9]|[1-2][0-9]|[3][0-1])\/([1-9]|[0][1-9]|[1-2][0-9]|[3][0-1]))" ~ r"|" ~
              r"(?P<"~Dom.undef~r">[\?])" ~ r"|" ~
              r"(?P<"~Dom.any~r">[\*])";

enum monReg = r"(?P<"~Mon.list~r">(([1-9]|0[1-9]|1[0-2]),)*([1-9]|0[1-9]|1[0-2]))" ~ r"|" ~
              r"(?P<"~Mon.range~r">([1-9]|0[1-9]|1[0-2])-([1-9]|0[1-9]|1[0-2]))" ~ r"|" ~
              r"(?P<"~Mon.seq~r">([\*]|[1-9]|0[1-9]|1[0-2])\/([1-9]|0[1-9]|1[0-2]))" ~ r"|" ~
              r"(?P<"~Mon.listN~r">((" ~ moyNames ~ r"),)*(" ~ moyNames ~ r"))" ~ r"|" ~
              r"(?P<"~Mon.rangeN~r">(" ~ moyNames ~ r")-(" ~ moyNames ~ r"))" ~ r"|" ~
              r"(?P<"~Mon.seqN~r">(" ~ moyNames ~ r")\/([1-9]|0[1-9]|1[0-2]))" ~ r"|" ~
              r"(?P<"~Mon.any~r">[\*])";

enum dowReg = r"(?P<"~Dow.list~r">([1-7],)*([1-7]))" ~ r"|" ~
              r"(?P<"~Dow.range~r">[1-7]-[1-7])" ~ r"|" ~
              r"(?P<"~Dow.seq~r">([[\*]|[1-7]])\/([1-7]))" ~ r"|" ~
              r"(?P<"~Dow.listN~r">(("~ dowNames ~ r"),)*(" ~ dowNames ~ r"))" ~ r"|" ~
              r"(?P<"~Dow.rangeN~r">(" ~ dowNames ~ r")-(" ~ dowNames ~ r"))" ~ r"|" ~
              r"(?P<"~Dow.seqN~r">(" ~ dowNames ~ r")\/([1-7]))" ~ r"|" ~
              r"(?P<"~Dow.undef~r">[\?])" ~ r"|" ~
              r"(?P<"~Dow.any~r">[\*])";

enum cronReg = r"^(" ~
                    secReg ~ r")[\s](" ~ 
                    minReg ~ r")[\s](" ~
                    hrsReg ~ r")[\s](" ~
                    domReg ~ r")[\s](" ~
                    monReg ~ r")[\s](" ~
                    dowReg ~
                r")$";


enum secKeys = [EnumMembers!Sec];
enum minKeys = [EnumMembers!Min];
enum hrsKeys = [EnumMembers!Hrs];
enum domKeys = [EnumMembers!Dom];
enum monKeys = [EnumMembers!Mon];
enum dowKeys = [EnumMembers!Dow];


unittest
{
    auto expr = "10,11,12 0-12 */12 ? JAN/3 *";

    auto regexp = ctRegex!(cronReg);
    auto match = matchFirst(expr, regexp);

    assert(match[cast(string)Sec.list].length);
    assert(match[cast(string)Min.range].length);
    assert(match[cast(string)Hrs.seq].length);
    assert(match[cast(string)Dom.undef].length);
    assert(match[cast(string)Mon.seqN].length);
    assert(match[cast(string)Dow.any].length);
}



/**
  * Struct containing parsed cron expression
  */
struct CronExpr
{
    ulong  seconds;
    ulong  minutes;
    uint   hours;
    uint   doms;
    ushort months;
    ubyte  dows;


    static auto opCall(R)(R expr)
        if (isSomeString!R)
    {
        CronExpr cron;

        auto regexp = ctRegex!(cronReg);
        auto match = matchFirst(expr, regexp);

        if (match.empty)
            throw new CronException("Invalid cron expression");

        cron.parseSeconds!R(match);
        cron.parseMinutes!R(match);
        cron.parseHours!R(match);
        cron.parseDaysOfMonth!R(match);
        cron.parseMonths!R(match);
        cron.parseDaysOfWeek(match);

        return cron;
    }

    /**
      * Get next date of execution after current
      */
    Nullable!DateTime getNext(DateTime current) const
    {
        DateTime next = current;
        next += 1.seconds;

        uint ind = 0;
        while (true)
        {
            // Break loop after 4 years
            if (ind > 4 * 366)
                return Nullable!DateTime.init;

            // Find next valid month
            if (!bitTest(months, next.month))
            {
                ind += next.daysInMonth;
                next.add!"months"(1);
                next.day = 1;
                next.hour = 0;
                next.minute = 0;
                next.second = 0;
                continue;
            }

            // Find next valid day of month
            if (!bitTest(doms, next.day))
            {
                ind += 1;
                next += 1.days;
                next.hour = 0;
                next.minute = 0;
                next.second = 0;
                continue;
            }

            // Find next valid day of week
            if (!bitTest(dows, next.dow))
            {
                ind += 1;
                next += 1.days;
                next.hour = 0;
                next.minute = 0;
                next.second = 0;
                continue;
            }

            // Find next valid hour
            if (!bitTest(hours, next.hour))
            {
                next += 1.hours;
                next.minute = 0;
                next.second = 0;
                continue;
            }

            // Find next valid minute
            if (!bitTest(minutes, next.minute))
            {
                next += 1.minutes;
                next.second = 0;
                continue;
            }

            // Find next valid second
            if (!bitTest(seconds, next.second))
            {
                next += 1.seconds;
                continue;
            }
            
           break; 
        }

        return Nullable!DateTime(next);
    }


private:


    /**
      * Method for parsing `seconds` part of expression
      */
    void parseSeconds(R)(ref Captures!R match)
    {
        auto keys = secKeys
                    .filter!(a => match[cast(string)a].length)
                    .array;

        if (!keys.length)
            throw new CronException("Undefined expression while parsing seconds");

        auto expr = match[cast(string)(keys[0])];
        final switch (keys[0]) with (Sec)
        {
            case list:
                parseList(seconds, expr);
                return;
            case range:
                parseRange(seconds, expr, Range.sec.from, Range.sec.to);
                return;
            case seq:
                parseSequence(seconds, expr, Range.sec.from, Range.sec.to);
                return;
            case any:
                parseAny(seconds, Range.sec.from, Range.sec.to);
                return;
        }
    }


    /**
      * Method for parsing `minutes` part of expression
      */
    void parseMinutes(R)(ref Captures!R match)
    {
        auto keys = minKeys
                    .filter!(a => match[cast(string)a].length)
                    .array;

        if (!keys.length)
            throw new CronException("Undefined expression while parsing minutes");

        auto expr = match[cast(string)(keys[0])];
        final switch (keys[0]) with (Min)
        {
            case list:
                parseList(minutes, expr);
                return;
            case range:
                parseRange(minutes, expr, Range.min.from, Range.min.to);
                return;
            case seq:
                parseSequence(minutes, expr, Range.min.from, Range.min.to);
                return;
            case any:
                parseAny(minutes, Range.min.from, Range.min.to);
                return;
        }
    }


    /**
      * Method for parsing `hours` part of expression
      */
    void parseHours(R)(ref Captures!R match)
    {
        auto keys = hrsKeys
                    .filter!(a => match[cast(string)a].length)
                    .array;

        if (!keys.length)
            throw new CronException("Undefined expression while parsing hours");

        auto expr = match[cast(string)(keys[0])];
        final switch (keys[0]) with (Hrs)
        {
            case list:
                parseList(hours, expr);
                return;
            case range:
                parseRange(hours, expr, Range.hrs.from, Range.hrs.to);
                return;
            case seq:
                parseSequence(hours, expr, Range.hrs.from, Range.hrs.to);
                return;
            case any:
                parseAny(hours, Range.hrs.from, Range.hrs.to);
                return;
        }
    }


    /**
      * Method for parsing `days of month` part of expression
      */
    void parseDaysOfMonth(R)(ref Captures!R match)
    {
        auto keys = domKeys
                    .filter!(a => match[cast(string)a].length)
                    .array;

        if (!keys.length)
            throw new CronException("Undefined expression while parsing day of months");

        auto expr = match[cast(string)(keys[0])];
        final switch (keys[0]) with (Dom)
        {
            case list:
                parseList(doms, expr);
                return;
            case range:
                parseRange(doms, expr, Range.dom.from, Range.dom.to);
                return;
            case seq:
                parseSequence(doms, expr, Range.dom.from, Range.dom.to);
                return;
            case undef:
            case any:
                parseAny(doms, Range.dom.from, Range.dom.to);
                return;
        }
    }


    /**
      * Method for parsing `months` part of expression
      */
    void parseMonths(R)(ref Captures!R match)
    {
        auto keys = monKeys
                    .filter!(a => match[cast(string)a].length)
                    .array;

        if (!keys.length)
            throw new CronException("Undefined expression while parsing months");

        auto expr = match[cast(string)(keys[0])];
        final switch (keys[0]) with (Mon)
        {
            case list:
                parseList(months, expr);
                return;
            case listN:
                parseList(months, expr.replaceNames(moyNames));
                return;
            case range:
                parseRange(months, expr, Range.mon.from, Range.mon.to);
                return;
            case rangeN:
                parseRange(months, expr.replaceNames(moyNames), Range.mon.from, Range.mon.to);
                return;
            case seq:
                parseSequence(months, expr, Range.mon.from, Range.mon.to);
                return;
            case seqN:
                parseSequence(months, expr.replaceNames(moyNames), Range.mon.from, Range.mon.to);
                return;
            case any:
                parseAny(months, Range.mon.from, Range.mon.to);
                return;
        }
    }


    /**
      * Method for parsing `days of week` part of expression
      */
    void parseDaysOfWeek(R)(ref Captures!R match)
    {
        auto keys = dowKeys
                    .filter!(a => match[cast(string)a].length)
                    .array;

        if (!keys.length)
            throw new CronException("Undefined expression while parsing days of week");

        auto expr = match[cast(string)(keys[0])];
        final switch (keys[0]) with (Dow)
        {
            case list:
                parseList(dows, expr);
                return;
            case listN:
                parseList(dows, expr.replaceNames(dowNames));
                return;
            case range:
                parseRange(dows, expr, Range.dow.from, Range.dow.to);
                return;
            case rangeN:
                parseRange(dows, expr.replaceNames(dowNames), Range.dow.from, Range.dow.to);
                return;
            case seq:
                parseSequence(dows, expr, Range.dow.from, Range.dow.to);
                return;
            case seqN:
                parseSequence(dows, expr.replaceNames(dowNames), Range.dow.from, Range.dow.to);
                return;
            case undef:
            case any:
                parseAny(dows, Range.dow.from, Range.dow.to);
                return;
        }
    }
}


unittest
{
    auto c1 = CronExpr("30 0-1 12 1/2 NOV-FEB 2/2");
    auto d1 = DateTime(2000, 6, 1, 10, 30, 0);
    assert(c1.getNext(d1).get == DateTime(2000, 11, 7, 12, 00, 30));

    auto c2 = CronExpr("0 0 0 29 FEB THU");
    auto d2 = DateTime(2000, 1, 1, 0, 0, 0);
    assert(c2.getNext(d2).isNull);
}


/**
  * Cron exception
  */
class CronException : Exception
{
    this(string msg, string file = __FILE__, size_t line = __LINE__)
    {
        super(msg, file, line);
    }
}


/**
  * Replace names of dows and doms in expression
  */
auto replaceNames(R)(R expr, string names)
    if (isSomeString!R)
{
    import std.array : replace; 

    R result = expr;

    ubyte i = 0;
    names
        .split("|")
        .map!(a => tuple(a, to!string(++i)))
        .each!(a => result = replace(result, a[0], a[1]));

    return result;
}


unittest
{
    auto test = "JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC MON,TUE,WED,THU,FRI,SAT,SUN";
    auto expected = "1,2,3,4,5,6,7,8,9,10,11,12 1,2,3,4,5,6,7";
    auto result = test
                    .replaceNames(moyNames)
                    .replaceNames(dowNames);
    assert(result == expected);
}
