/**
  * This module contains some utils usefull for cron parser
  *
  * Copyright:
  *     Copyright (c) 2018, Maxim Tyapkin.
  * Authors:
  *     Maxim Tyapkin
  * License:
  *     This software is licensed under the terms of the BSD 3-clause license.
  *     The full terms of the license can be found in the LICENSE.md file.
  */

module scheduled.cronexp.utils;

private
{
    import std.algorithm : each, map;
    import std.array : array, split;
    import std.conv : to;
    import std.datetime;
    import std.range : iota;
    import std.traits : isSomeString;
    import std.typecons : tuple;
}


/**
  * Parse list of indexes to var (eg. "1,2,3,5,8")
  */
void parseList(T, R)(ref T val, R expr)
    if (isSomeString!R)
{
    val = 0;
    expr.split(",")
        .map!(to!ubyte)
        .array
        .each!(n => bitSet!T(val, n));
}


unittest
{
    ushort x;
    parseList(x, "1,2,4,5,8");
    assert(x == cast(ushort)0b100110110);
}


/**
  * Parse range of indexes to var (eg. "2-4" or "5-1")
  *
  * Note:
  * If the range "a-b" is reversed (a > b), it's mean
  * two ranges: "a-until", "from-b"
  */
void parseRange(T, R)(ref T val, R expr, ubyte from, ubyte until)
    if (isSomeString!R)
{
    val = 0;
    auto rngs = expr.split("-")
                    .map!(to!ubyte)
                    .array;
    if (rngs.length < 2)
        return;

    if (rngs[0] < rngs[1])
    {
        iota(rngs[0], rngs[1] + 1)
            .each!(n => bitSet!T(val, cast(ubyte)n));
    }
    else
    {
        iota(rngs[0], until + 1)
            .each!(n => bitSet!T(val, cast(ubyte)n));
        iota(from, rngs[1] + 1)
            .each!(n => bitSet!T(val, cast(ubyte)n));
    }
}


unittest
{
    ushort x;
    parseRange(x, "4-7", 1, 10);
    assert(x == cast(ushort)0b_0000_0000_1111_0000);
    parseRange(x, "7-4", 1, 10);
    assert(x == cast(ushort)0b_0000_0111_1001_1110);
}


/**
  * Parse sequence of indexes to var (eg. "a/x" == "a, a+x, a+2x, ...")
  *
  * Note: "*" is synonym for "from"
  */
void parseSequence(T, R)(ref T val, R expr, ubyte from, ubyte until)
    if (isSomeString!R)
{
    val = 0;
    auto rngs = expr.split("/")
                    .map!(n => n == "*" ? from : n.to!ubyte)
                    .array;
    if(rngs.length < 2)
        return;

    iota(rngs[0], until + 1, rngs[1])
        .each!(n => bitSet!T(val, cast(ubyte)n));
}


unittest
{
    ushort x;
    parseSequence(x, "1/3", 1, 10);
    assert(x == cast(ushort)0b_0000_0100_1001_0010);
    parseSequence(x, "*/2", 0, 15);
    assert(x == cast(ushort)0b_0101_0101_0101_0101);
    parseSequence(x, "2/10", 0, 7);
    assert(x == cast(ushort)0b_0000_0000_0000_0100);
}


/**
  * Fill all allowed indexes to var
  */
void parseAny(T)(ref T val, ubyte from, ubyte until)
{
    val = 0;
    iota(from, until + 1)
        .each!(n => bitSet!T(val, cast(ubyte)n));
}


unittest
{
    ushort x;
    parseAny(x, 1, 10);
    assert(x == cast(ushort)0b_0000_0111_1111_1110);
}


/**
  * Get idx-th bit in var
  */
bool bitTest(T)(T num, ubyte idx)
{
    return (num & (cast(T)1 << idx)) != 0;
}


/**
  * Set idx-th bit in var to 1
  */
void bitSet(T)(ref T num, ubyte idx)
{
    num = cast(T)(num | (cast(T)1 << idx));
}


/**
  * Set idx-th bit in var to 0
  */
void bitClear(T)(ref T num, ubyte idx)
{
    num = cast(T)(num & (~(cast(T)1 << idx)));
}


unittest
{
    ulong num = 0x1_00_00;
    assert(bitTest(num, 16));
    bitClear(num, 16);
    assert(!bitTest(num, 16));
    bitSet(num, 63);
    assert(num);
}


/**
  * Convert std.datetime.DayOfWeek enum to dow index
  */
ubyte dow(DateTime dt)
{
    final switch (dt.dayOfWeek) with (DayOfWeek)
    {
        case mon: return 1;
        case tue: return 2;
        case wed: return 3;
        case thu: return 4;
        case fri: return 5;
        case sat: return 6;
        case sun: return 7;
    }
}


unittest
{
    //3 JAN 2000 - monday
    assert(DateTime(2000, 1, 3).dow == 1);
    assert(DateTime(2000, 1, 4).dow == 2);
    assert(DateTime(2000, 1, 5).dow == 3);
    assert(DateTime(2000, 1, 6).dow == 4);
    assert(DateTime(2000, 1, 7).dow == 5);
    assert(DateTime(2000, 1, 8).dow == 6);
    assert(DateTime(2000, 1, 9).dow == 7);
}
