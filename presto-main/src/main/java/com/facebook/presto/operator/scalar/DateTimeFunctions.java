/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntervalDayTimeType;
import com.facebook.presto.spi.type.IntervalYearMonthType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.util.DateTimeZoneIndex;
import com.facebook.presto.util.ThreadLocalCache;
import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.util.Locale;

import static com.facebook.presto.operator.scalar.QuarterOfYearDateTimeField.QUARTER_OF_YEAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateTimeEncoding.updateMillisUtc;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeZoneIndex.extractZoneOffsetMinutes;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static com.facebook.presto.util.DateTimeZoneIndex.unpackChronology;

public final class DateTimeFunctions
{
    private static final ThreadLocalCache<Slice, DateTimeFormatter> DATETIME_FORMATTER_CACHE = new ThreadLocalCache<Slice, DateTimeFormatter>(100)
    {
        @Override
        protected DateTimeFormatter load(Slice format)
        {
            return createDateTimeFormatter(format);
        }
    };

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(DateTimeZone.UTC);
    private static final DateTimeField SECOND_OF_MINUTE = UTC_CHRONOLOGY.secondOfMinute();
    private static final DateTimeField DAY_OF_WEEK = UTC_CHRONOLOGY.dayOfWeek();
    private static final DateTimeField DAY_OF_MONTH = UTC_CHRONOLOGY.dayOfMonth();
    private static final DateTimeField DAY_OF_YEAR = UTC_CHRONOLOGY.dayOfYear();
    private static final DateTimeField WEEK_OF_YEAR = UTC_CHRONOLOGY.weekOfWeekyear();
    private static final DateTimeField MONTH_OF_YEAR = UTC_CHRONOLOGY.monthOfYear();
    private static final DateTimeField QUARTER = QUARTER_OF_YEAR.getField(UTC_CHRONOLOGY);
    private static final DateTimeField YEAR = UTC_CHRONOLOGY.year();
    private static final int MILLISECONDS_IN_SECOND = 1000;
    private static final int MILLISECONDS_IN_MINUTE = 60 * MILLISECONDS_IN_SECOND;
    private static final int MILLISECONDS_IN_HOUR = 60 * MILLISECONDS_IN_MINUTE;
    private static final int MILLISECONDS_IN_DAY = 24 * MILLISECONDS_IN_HOUR;

    private DateTimeFunctions() {}

    @Description("current date")
    @ScalarFunction
    @SqlType(DateType.class)
    public static long currentDate(ConnectorSession session)
    {
        // Stack value is the millisecond at midnight on the date in UTC so
        // no we do not want to correct to the local client time zone.  Client
        // time zone corrections are not needed for date.
        return UTC_CHRONOLOGY.dayOfMonth().roundFloor(session.getStartTime());
    }

    @Description("current time with time zone")
    @ScalarFunction
    @SqlType(TimeWithTimeZoneType.class)
    public static long currentTime(ConnectorSession session)
    {
        // Stack value is number of milliseconds from start of the current day,
        // but the start of the day is relative to the current time zone.
        long millis = getChronology(session.getTimeZoneKey()).millisOfDay().get(session.getStartTime());
        return packDateTimeWithZone(millis, session.getTimeZoneKey());
    }

    @Description("current time without time zone")
    @ScalarFunction("localtime")
    @SqlType(TimeType.class)
    public static long localTime(ConnectorSession session)
    {
        // Stack value is number of milliseconds from start of the current day,
        // but the start of the day is relative to the current time zone.
        return getChronology(session.getTimeZoneKey()).millisOfDay().get(session.getStartTime());
    }

    @Description("current timestamp with time zone")
    @ScalarFunction(value = "current_timestamp", alias = "now")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long currentTimestamp(ConnectorSession session)
    {
        return packDateTimeWithZone(session.getStartTime(), session.getTimeZoneKey());
    }

    @Description("current timestamp without time zone")
    @ScalarFunction("localtimestamp")
    @SqlType(TimestampType.class)
    public static long localTimestamp(ConnectorSession session)
    {
        return session.getStartTime();
    }

    @ScalarFunction("from_unixtime")
    @SqlType(TimestampType.class)
    public static long fromUnixTime(@SqlType(DoubleType.class) double unixTime)
    {
        return Math.round(unixTime * 1000);
    }

    @ScalarFunction("from_unixtime")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long fromUnixTime(@SqlType(DoubleType.class) double unixTime, @SqlType(BigintType.class) long hoursOffset, @SqlType(BigintType.class) long minutesOffset)
    {
        return packDateTimeWithZone(Math.round(unixTime * 1000), (int) (hoursOffset * 60 + minutesOffset));
    }

    @ScalarFunction("to_unixtime")
    @SqlType(DoubleType.class)
    public static double toUnixTime(@SqlType(TimestampType.class) long timestamp)
    {
        return timestamp / 1000.0;
    }

    @ScalarFunction("to_unixtime")
    @SqlType(DoubleType.class)
    public static double toUnixTimeFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return unpackMillisUtc(timestampWithTimeZone) / 1000.0;
    }

    @ScalarFunction(value = "at_timezone")
    @SqlType(TimeWithTimeZoneType.class)
    public static long timeAtTimeZone(@SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone, @SqlType(VarcharType.class) Slice zoneId)
    {
        return packDateTimeWithZone(unpackMillisUtc(timeWithTimeZone), zoneId.toStringUtf8());
    }

    @ScalarFunction(value = "at_timezone")
    @SqlType(TimeWithTimeZoneType.class)
    public static long timeAtTimeZone(@SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone, @SqlType(IntervalDayTimeType.class) long zoneOffset)
    {
        if (zoneOffset % 60_000 != 0) {
            throw new IllegalArgumentException("Invalid time zone offset interval: interval contains seconds");
        }

        int zoneOffsetMinutes = (int) (zoneOffset / 60_000);
        return packDateTimeWithZone(unpackMillisUtc(timeWithTimeZone), getTimeZoneKeyForOffset(zoneOffsetMinutes));
    }

    @ScalarFunction(value = "at_timezone")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long timestampAtTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone, @SqlType(VarcharType.class) Slice zoneId)
    {
        return packDateTimeWithZone(unpackMillisUtc(timestampWithTimeZone), zoneId.toStringUtf8());
    }

    @ScalarFunction(value = "at_timezone")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long timestampAtTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone, @SqlType(IntervalDayTimeType.class) long zoneOffset)
    {
        if (zoneOffset % 60_000 != 0) {
            throw new IllegalArgumentException("Invalid time zone offset interval: interval contains seconds");
        }

        int zoneOffsetMinutes = (int) (zoneOffset / 60_000);
        return packDateTimeWithZone(unpackMillisUtc(timestampWithTimeZone), getTimeZoneKeyForOffset(zoneOffsetMinutes));
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("date_trunc")
    @SqlType(DateType.class)
    public static long truncateDate(ConnectorSession session, @SqlType(VarcharType.class) Slice unit, @SqlType(DateType.class) long time)
    {
        return getDateField(UTC_CHRONOLOGY, unit).roundFloor(time);
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("date_trunc")
    @SqlType(TimeType.class)
    public static long truncateTime(ConnectorSession session, @SqlType(VarcharType.class) Slice unit, @SqlType(TimeType.class) long time)
    {
        return getTimeField(getChronology(session.getTimeZoneKey()), unit).roundFloor(time);
    }

    @Description("truncate to the specified precision")
    @ScalarFunction("date_trunc")
    @SqlType(TimeWithTimeZoneType.class)
    public static long truncateTimeWithTimeZone(@SqlType(VarcharType.class) Slice unit, @SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone)
    {
        long millis = getTimeField(unpackChronology(timeWithTimeZone), unit).roundFloor(unpackMillisUtc(timeWithTimeZone));
        return updateMillisUtc(millis, timeWithTimeZone);
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("date_trunc")
    @SqlType(TimestampType.class)
    public static long truncateTimestamp(ConnectorSession session, @SqlType(VarcharType.class) Slice unit, @SqlType(TimestampType.class) long timestamp)
    {
        return getTimestampField(getChronology(session.getTimeZoneKey()), unit).roundFloor(timestamp);
    }

    @Description("truncate to the specified precision")
    @ScalarFunction("date_trunc")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long truncateTimestampWithTimezone(@SqlType(VarcharType.class) Slice unit, @SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        long millis = getTimestampField(unpackChronology(timestampWithTimeZone), unit).roundFloor(unpackMillisUtc(timestampWithTimeZone));
        return updateMillisUtc(millis, timestampWithTimeZone);
    }

    @Description("add the specified amount of date to the given date")
    @ScalarFunction("date_add")
    @SqlType(DateType.class)
    public static long addFieldValueDate(ConnectorSession session, @SqlType(VarcharType.class) Slice unit, @SqlType(BigintType.class) long value, @SqlType(DateType.class) long date)
    {
        return getDateField(UTC_CHRONOLOGY, unit).add(date, Ints.checkedCast(value));
    }

    @Description("add the specified amount of time to the given time")
    @ScalarFunction("date_add")
    @SqlType(TimeType.class)
    public static long addFieldValueTime(ConnectorSession session, @SqlType(VarcharType.class) Slice unit, @SqlType(BigintType.class) long value, @SqlType(TimeType.class) long time)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        return modulo24Hour(chronology, getTimeField(chronology, unit).add(time, Ints.checkedCast(value)));
    }

    @Description("add the specified amount of time to the given time")
    @ScalarFunction("date_add")
    @SqlType(TimeWithTimeZoneType.class)
    public static long addFieldValueTimeWithTimeZone(
            @SqlType(VarcharType.class) Slice unit,
            @SqlType(BigintType.class) long value,
            @SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone)
    {
        ISOChronology chronology = unpackChronology(timeWithTimeZone);
        long millis = modulo24Hour(chronology, getTimeField(chronology, unit).add(unpackMillisUtc(timeWithTimeZone), Ints.checkedCast(value)));
        return updateMillisUtc(millis, timeWithTimeZone);
    }

    @Description("add the specified amount of time to the given timestamp")
    @ScalarFunction("date_add")
    @SqlType(TimestampType.class)
    public static long addFieldValueTimestamp(
            ConnectorSession session,
            @SqlType(VarcharType.class) Slice unit,
            @SqlType(BigintType.class) long value,
            @SqlType(TimestampType.class) long timestamp)
    {
        return getTimestampField(getChronology(session.getTimeZoneKey()), unit).add(timestamp, Ints.checkedCast(value));
    }

    @Description("add the specified amount of time to the given timestamp")
    @ScalarFunction("date_add")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long addFieldValueTimestampWithTimeZone(
            @SqlType(VarcharType.class) Slice unit,
            @SqlType(BigintType.class) long value,
            @SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        long millis = getTimestampField(unpackChronology(timestampWithTimeZone), unit).add(unpackMillisUtc(timestampWithTimeZone), Ints.checkedCast(value));
        return updateMillisUtc(millis, timestampWithTimeZone);
    }

    @Description("difference of the given dates in the given unit")
    @ScalarFunction("date_diff")
    @SqlType(BigintType.class)
    public static long diffDate(ConnectorSession session, @SqlType(VarcharType.class) Slice unit, @SqlType(DateType.class) long date1, @SqlType(DateType.class) long date2)
    {
        return getDateField(UTC_CHRONOLOGY, unit).getDifference(date2, date1);
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    @SqlType(BigintType.class)
    public static long diffTime(ConnectorSession session, @SqlType(VarcharType.class) Slice unit, @SqlType(TimeType.class) long time1, @SqlType(TimeType.class) long time2)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        return getTimeField(chronology, unit).getDifference(time2, time1);
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    @SqlType(BigintType.class)
    public static long diffTimeWithTimeZone(
            @SqlType(VarcharType.class) Slice unit,
            @SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone1,
            @SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone2)
    {
        return getTimeField(unpackChronology(timeWithTimeZone1), unit).getDifference(unpackMillisUtc(timeWithTimeZone2), unpackMillisUtc(timeWithTimeZone1));
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    @SqlType(BigintType.class)
    public static long diffTimestamp(
            ConnectorSession session,
            @SqlType(VarcharType.class) Slice unit,
            @SqlType(TimestampType.class) long timestamp1,
            @SqlType(TimestampType.class) long timestamp2)
    {
        return getTimestampField(getChronology(session.getTimeZoneKey()), unit).getDifference(timestamp2, timestamp1);
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    @SqlType(BigintType.class)
    public static long diffTimestampWithTimeZone(
            @SqlType(VarcharType.class) Slice unit,
            @SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone1,
            @SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone2)
    {
        return getTimestampField(unpackChronology(timestampWithTimeZone1), unit).getDifference(unpackMillisUtc(timestampWithTimeZone2), unpackMillisUtc(timestampWithTimeZone1));
    }

    private static DateTimeField getDateField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toString(Charsets.UTF_8).toLowerCase();
        switch (unitString) {
            case "day":
                return chronology.dayOfMonth();
            case "week":
                return chronology.weekOfWeekyear();
            case "month":
                return chronology.monthOfYear();
            case "quarter":
                return QUARTER_OF_YEAR.getField(chronology);
            case "year":
                return chronology.year();
            default:
                throw new IllegalArgumentException("'" + unitString + "' is not a valid DATE field");
        }
    }

    private static DateTimeField getTimeField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toString(Charsets.UTF_8).toLowerCase();
        switch (unitString) {
            case "second":
                return chronology.secondOfMinute();
            case "minute":
                return chronology.minuteOfHour();
            case "hour":
                return chronology.hourOfDay();
            default:
                throw new IllegalArgumentException("'" + unitString + "' is not a valid Time field");
        }
    }

    private static DateTimeField getTimestampField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toString(Charsets.UTF_8).toLowerCase();
        switch (unitString) {
            case "second":
                return chronology.secondOfMinute();
            case "minute":
                return chronology.minuteOfHour();
            case "hour":
                return chronology.hourOfDay();
            case "day":
                return chronology.dayOfMonth();
            case "week":
                return chronology.weekOfWeekyear();
            case "month":
                return chronology.monthOfYear();
            case "quarter":
                return QUARTER_OF_YEAR.getField(chronology);
            case "year":
                return chronology.year();
            default:
                throw new IllegalArgumentException("'" + unitString + "' is not a valid Timestamp field");
        }
    }

    @Description("parses the specified date/time by the given format")
    @ScalarFunction
    @SqlType(TimestampWithTimeZoneType.class)
    public static long parseDatetime(ConnectorSession session, @SqlType(VarcharType.class) Slice datetime, @SqlType(VarcharType.class) Slice formatString)
    {
        String pattern = formatString.toString(Charsets.UTF_8);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern)
                .withChronology(getChronology(session.getTimeZoneKey()))
                .withOffsetParsed()
                .withLocale(session.getLocale());

        String datetimeString = datetime.toString(Charsets.UTF_8);
        return DateTimeZoneIndex.packDateTimeWithZone(parseDateTimeHelper(formatter, datetimeString));
    }

    private static DateTime parseDateTimeHelper(DateTimeFormatter formatter, String datetimeString)
    {
        try {
            return formatter.parseDateTime(datetimeString);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT.toErrorCode(), e);
        }
    }

    @Description("formats the given time by the given format")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice formatDatetime(ConnectorSession session, @SqlType(TimestampType.class) long timestamp, @SqlType(VarcharType.class) Slice formatString)
    {
        return formatDatetime(getChronology(session.getTimeZoneKey()), session.getLocale(), timestamp, formatString);
    }

    @Description("formats the given time by the given format")
    @ScalarFunction("format_datetime")
    @SqlType(VarcharType.class)
    public static Slice formatDatetimeWithTimeZone(
            ConnectorSession session,
            @SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone,
            @SqlType(VarcharType.class) Slice formatString)
    {
        return formatDatetime(unpackChronology(timestampWithTimeZone), session.getLocale(), unpackMillisUtc(timestampWithTimeZone), formatString);
    }

    private static Slice formatDatetime(ISOChronology chronology, Locale locale, long timestamp, Slice formatString)
    {
        String pattern = formatString.toString(Charsets.UTF_8);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern)
                .withChronology(chronology)
                .withLocale(locale);

        String datetimeString = formatter.print(timestamp);
        return Slices.wrappedBuffer(datetimeString.getBytes(Charsets.UTF_8));
    }

    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice dateFormat(ConnectorSession session, @SqlType(TimestampType.class) long timestamp, @SqlType(VarcharType.class) Slice formatString)
    {
        return dateFormat(getChronology(session.getTimeZoneKey()), session.getLocale(), timestamp, formatString);
    }

    @ScalarFunction("date_format")
    @SqlType(VarcharType.class)
    public static Slice dateFormatWithTimeZone(
            ConnectorSession session,
            @SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone,
            @SqlType(VarcharType.class) Slice formatString)
    {
        return dateFormat(unpackChronology(timestampWithTimeZone), session.getLocale(), unpackMillisUtc(timestampWithTimeZone), formatString);
    }

    private static Slice dateFormat(ISOChronology chronology, Locale locale, long timestamp, Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString)
                .withChronology(chronology)
                .withLocale(locale);

        return Slices.copiedBuffer(formatter.print(timestamp), Charsets.UTF_8);
    }

    @ScalarFunction
    @SqlType(TimestampType.class)
    public static long dateParse(ConnectorSession session, @SqlType(VarcharType.class) Slice dateTime, @SqlType(VarcharType.class) Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString)
                .withChronology(getChronology(session.getTimeZoneKey()))
                .withLocale(session.getLocale());

        try {
            return formatter.parseMillis(dateTime.toString(Charsets.UTF_8));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT.toErrorCode(), e);
        }
    }

    @Description("second of the minute of the given timestamp")
    @ScalarFunction("second")
    @SqlType(BigintType.class)
    public static long secondFromTimestamp(@SqlType(TimestampType.class) long timestamp)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(timestamp);
    }

    @Description("second of the minute of the given timestamp")
    @ScalarFunction("second")
    @SqlType(BigintType.class)
    public static long secondFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("second of the minute of the given time")
    @ScalarFunction("second")
    @SqlType(BigintType.class)
    public static long secondFromTime(@SqlType(TimeType.class) long time)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(time);
    }

    @Description("second of the minute of the given time")
    @ScalarFunction("second")
    @SqlType(BigintType.class)
    public static long secondFromTimeWithTimeZone(@SqlType(TimeWithTimeZoneType.class) long time)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(unpackMillisUtc(time));
    }

    @Description("second of the minute of the given interval")
    @ScalarFunction("second")
    @SqlType(BigintType.class)
    public static long secondFromInterval(@SqlType(IntervalDayTimeType.class) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_MINUTE) / MILLISECONDS_IN_SECOND;
    }

    @Description("minute of the hour of the given timestamp")
    @ScalarFunction("minute")
    @SqlType(BigintType.class)
    public static long minuteFromTimestamp(ConnectorSession session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).minuteOfHour().get(timestamp);
    }

    @Description("minute of the hour of the given timestamp")
    @ScalarFunction("minute")
    @SqlType(BigintType.class)
    public static long minuteFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).minuteOfHour().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("minute of the hour of the given time")
    @ScalarFunction("minute")
    @SqlType(BigintType.class)
    public static long minuteFromTime(ConnectorSession session, @SqlType(TimeType.class) long time)
    {
        return getChronology(session.getTimeZoneKey()).minuteOfHour().get(time);
    }

    @Description("minute of the hour of the given time")
    @ScalarFunction("minute")
    @SqlType(BigintType.class)
    public static long minuteFromTimeWithTimeZone(@SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone)
    {
        return unpackChronology(timeWithTimeZone).minuteOfHour().get(unpackMillisUtc(timeWithTimeZone));
    }

    @Description("minute of the hour of the given interval")
    @ScalarFunction("minute")
    @SqlType(BigintType.class)
    public static long minuteFromInterval(@SqlType(IntervalDayTimeType.class) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_HOUR) / MILLISECONDS_IN_MINUTE;
    }

    @Description("hour of the day of the given timestamp")
    @ScalarFunction("hour")
    @SqlType(BigintType.class)
    public static long hourFromTimestamp(ConnectorSession session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).hourOfDay().get(timestamp);
    }

    @Description("hour of the day of the given timestamp")
    @ScalarFunction("hour")
    @SqlType(BigintType.class)
    public static long hourFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).hourOfDay().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("hour of the day of the given time")
    @ScalarFunction("hour")
    @SqlType(BigintType.class)
    public static long hourFromTime(ConnectorSession session, @SqlType(TimeType.class) long time)
    {
        return getChronology(session.getTimeZoneKey()).hourOfDay().get(time);
    }

    @Description("hour of the day of the given time")
    @ScalarFunction("hour")
    @SqlType(BigintType.class)
    public static long hourFromTimeWithTimeZone(@SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone)
    {
        return unpackChronology(timeWithTimeZone).hourOfDay().get(unpackMillisUtc(timeWithTimeZone));
    }

    @Description("hour of the day of the given interval")
    @ScalarFunction("hour")
    @SqlType(BigintType.class)
    public static long hourFromInterval(@SqlType(IntervalDayTimeType.class) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_DAY) / MILLISECONDS_IN_HOUR;
    }

    @Description("day of the week of the given timestamp")
    @ScalarFunction(value = "day_of_week", alias = "dow")
    @SqlType(BigintType.class)
    public static long dayOfWeekFromTimestamp(ConnectorSession session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfWeek().get(timestamp);
    }

    @Description("day of the week of the given timestamp")
    @ScalarFunction(value = "day_of_week", alias = "dow")
    @SqlType(BigintType.class)
    public static long dayOfWeekFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).dayOfWeek().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("day of the week of the given date")
    @ScalarFunction(value = "day_of_week", alias = "dow")
    @SqlType(BigintType.class)
    public static long dayOfWeekFromDate(@SqlType(DateType.class) long date)
    {
        return DAY_OF_WEEK.get(date);
    }

    @Description("day of the month of the given timestamp")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(BigintType.class)
    public static long dayFromTimestamp(ConnectorSession session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfMonth().get(timestamp);
    }

    @Description("day of the month of the given timestamp")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(BigintType.class)
    public static long dayFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).dayOfMonth().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("day of the month of the given date")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(BigintType.class)
    public static long dayFromDate(@SqlType(DateType.class) long date)
    {
        return DAY_OF_MONTH.get(date);
    }

    @Description("day of the month of the given interval")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(BigintType.class)
    public static long dayFromInterval(@SqlType(IntervalDayTimeType.class) long milliseconds)
    {
        return milliseconds / MILLISECONDS_IN_DAY;
    }

    @Description("day of the year of the given timestamp")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    @SqlType(BigintType.class)
    public static long dayOfYearFromTimestamp(ConnectorSession session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfYear().get(timestamp);
    }

    @Description("day of the year of the given timestamp")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    @SqlType(BigintType.class)
    public static long dayOfYearFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).dayOfYear().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("day of the year of the given date")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    @SqlType(BigintType.class)
    public static long dayOfYearFromDate(@SqlType(DateType.class) long date)
    {
        return DAY_OF_YEAR.get(date);
    }

    @Description("week of the year of the given timestamp")
    @ScalarFunction(value = "week", alias = "week_of_year")
    @SqlType(BigintType.class)
    public static long weekFromTimestamp(ConnectorSession session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).weekOfWeekyear().get(timestamp);
    }

    @Description("week of the year of the given timestamp")
    @ScalarFunction(value = "week", alias = "week_of_year")
    @SqlType(BigintType.class)
    public static long weekFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).weekOfWeekyear().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("week of the year of the given date")
    @ScalarFunction(value = "week", alias = "week_of_year")
    @SqlType(BigintType.class)
    public static long weekFromDate(@SqlType(DateType.class) long date)
    {
        return WEEK_OF_YEAR.get(date);
    }

    @Description("month of the year of the given timestamp")
    @ScalarFunction("month")
    @SqlType(BigintType.class)
    public static long monthFromTimestamp(ConnectorSession session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).monthOfYear().get(timestamp);
    }

    @Description("month of the year of the given timestamp")
    @ScalarFunction("month")
    @SqlType(BigintType.class)
    public static long monthFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).monthOfYear().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("month of the year of the given date")
    @ScalarFunction("month")
    @SqlType(BigintType.class)
    public static long monthFromDate(@SqlType(DateType.class) long date)
    {
        return MONTH_OF_YEAR.get(date);
    }

    @Description("month of the year of the given interval")
    @ScalarFunction("month")
    @SqlType(BigintType.class)
    public static long monthFromInterval(@SqlType(IntervalYearMonthType.class) long months)
    {
        return months % 12;
    }

    @Description("quarter of the year of the given timestamp")
    @ScalarFunction("quarter")
    @SqlType(BigintType.class)
    public static long quarterFromTimestamp(ConnectorSession session, @SqlType(TimestampType.class) long timestamp)
    {
        return QUARTER_OF_YEAR.getField(getChronology(session.getTimeZoneKey())).get(timestamp);
    }

    @Description("quarter of the year of the given timestamp")
    @ScalarFunction("quarter")
    @SqlType(BigintType.class)
    public static long quarterFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return QUARTER_OF_YEAR.getField(unpackChronology(timestampWithTimeZone)).get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("quarter of the year of the given date")
    @ScalarFunction("quarter")
    @SqlType(BigintType.class)
    public static long quarterFromDate(@SqlType(DateType.class) long date)
    {
        return QUARTER.get(date);
    }

    @Description("year of the given timestamp")
    @ScalarFunction("year")
    @SqlType(BigintType.class)
    public static long yearFromTimestamp(ConnectorSession session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).year().get(timestamp);
    }

    @Description("year of the given timestamp")
    @ScalarFunction("year")
    @SqlType(BigintType.class)
    public static long yearFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).year().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("year of the given date")
    @ScalarFunction("year")
    @SqlType(BigintType.class)
    public static long yearFromDate(@SqlType(DateType.class) long date)
    {
        return YEAR.get(date);
    }

    @Description("year of the given interval")
    @ScalarFunction("year")
    @SqlType(BigintType.class)
    public static long yearFromInterval(@SqlType(IntervalYearMonthType.class) long months)
    {
        return months / 12;
    }

    @Description("time zone minute of the given timestamp")
    @ScalarFunction("timezone_minute")
    @SqlType(BigintType.class)
    public static long timeZoneMinuteFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return extractZoneOffsetMinutes(timestampWithTimeZone) % 60;
    }

    @Description("time zone hour of the given timestamp")
    @ScalarFunction("timezone_hour")
    @SqlType(BigintType.class)
    public static long timeZoneHourFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return extractZoneOffsetMinutes(timestampWithTimeZone) / 60;
    }

    @Description("get the largest of the given values")
    @ScalarFunction("greatest")
    @SqlType(TimestampType.class)
    public static long greatestTimestamp(@SqlType(TimestampType.class) long value1, @SqlType(TimestampType.class) long value2)
    {
        return value1 > value2 ? value1 : value2;
    }

    @Description("get the smallest of the given values")
    @ScalarFunction("least")
    @SqlType(TimestampType.class)
    public static long leastTimestamp(@SqlType(TimestampType.class) long value1, @SqlType(TimestampType.class) long value2)
    {
        return value1 < value2 ? value1 : value2;
    }

    @Description("get the largest of the given values")
    @ScalarFunction("greatest")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long greatestTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long value1, @SqlType(TimestampWithTimeZoneType.class) long value2)
    {
        return unpackMillisUtc(value1) > unpackMillisUtc(value2) ? value1 : value2;
    }

    @Description("get the smallest of the given values")
    @ScalarFunction("least")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long leastTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long value1, @SqlType(TimestampWithTimeZoneType.class) long value2)
    {
        return unpackMillisUtc(value1) < unpackMillisUtc(value2) ? value1 : value2;
    }

    @Description("get the largest of the given values")
    @ScalarFunction("greatest")
    @SqlType(DateType.class)
    public static long greatestDate(@SqlType(DateType.class) long value1, @SqlType(DateType.class) long value2)
    {
        return value1 > value2 ? value1 : value2;
    }

    @Description("get the smallest of the given values")
    @ScalarFunction("least")
    @SqlType(DateType.class)
    public static long leastDate(@SqlType(DateType.class) long value1, @SqlType(DateType.class) long value2)
    {
        return value1 < value2 ? value1 : value2;
    }

    @SuppressWarnings("fallthrough")
    public static DateTimeFormatter createDateTimeFormatter(Slice format)
    {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();

        String formatString = format.toString(Charsets.UTF_8);
        boolean escaped = false;
        for (int i = 0; i < format.length(); i++) {
            char character = formatString.charAt(i);

            if (escaped) {
                switch (character) {
                    case 'a': // %a Abbreviated weekday name (Sun..Sat)
                        builder.appendDayOfWeekShortText();
                        break;
                    case 'b': // %b Abbreviated month name (Jan..Dec)
                        builder.appendMonthOfYearShortText();
                        break;
                    case 'c': // %c Month, numeric (0..12)
                        builder.appendMonthOfYear(1);
                        break;
                    case 'd': // %d Day of the month, numeric (00..31)
                        builder.appendDayOfMonth(2);
                        break;
                    case 'e': // %e Day of the month, numeric (0..31)
                        builder.appendDayOfMonth(1);
                        break;
                    case 'f': // %f Microseconds (000000..999999)
                        builder.appendMillisOfSecond(6);
                        break;
                    case 'H': // %H Hour (00..23)
                        builder.appendHourOfDay(2);
                        break;
                    case 'h': // %h Hour (01..12)
                    case 'I': // %I Hour (01..12)
                        builder.appendClockhourOfHalfday(2);
                        break;
                    case 'i': // %i Minutes, numeric (00..59)
                        builder.appendMinuteOfHour(2);
                        break;
                    case 'j': // %j Day of year (001..366)
                        builder.appendDayOfYear(3);
                        break;
                    case 'k': // %k Hour (0..23)
                        builder.appendClockhourOfDay(1);
                        break;
                    case 'l': // %l Hour (1..12)
                        builder.appendClockhourOfHalfday(1);
                        break;
                    case 'M': // %M Month name (January..December)
                        builder.appendMonthOfYearText();
                        break;
                    case 'm': // %m Month, numeric (00..12)
                        builder.appendMonthOfYear(2);
                        break;
                    case 'p': // %p AM or PM
                        builder.appendHalfdayOfDayText();
                        break;
                    case 'r': // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
                        builder.appendClockhourOfHalfday(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2)
                                .appendLiteral(' ')
                                .appendHalfdayOfDayText();
                        break;
                    case 'S': // %S Seconds (00..59)
                    case 's': // %s Seconds (00..59)
                        builder.appendSecondOfMinute(2);
                        break;
                    case 'T': // %T Time, 24-hour (hh:mm:ss)
                        builder.appendHourOfDay(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2);
                        break;
                    case 'v': // %v Week (01..53), where Monday is the first day of the week; used with %x
                        builder.appendWeekOfWeekyear(2);
                        break;
                    case 'x': // %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
                        builder.appendWeekyear(4, 4);
                        break;
                    case 'W': // %W Weekday name (Sunday..Saturday)
                        builder.appendDayOfWeekText();
                        break;
                    case 'w': // %w Day of the week (0=Sunday..6=Saturday)
                        builder.appendDayOfWeek(1);
                        break;
                    case 'Y': // %Y Year, numeric, four digits
                        builder.appendYear(4, 4);
                        break;
                    case 'y': // %y Year, numeric (two digits)
                        builder.appendYearOfCentury(2, 2);
                        break;
                    case 'U': // %U Week (00..53), where Sunday is the first day of the week
                    case 'u': // %u Week (00..53), where Monday is the first day of the week
                    case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
                    case 'X': // %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
                    case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
                        throw new PrestoException(INVALID_FUNCTION_ARGUMENT.toErrorCode(), String.format("%%%s not supported in date format string", character));
                    case '%': // %% A literal “%” character
                        builder.appendLiteral('%');
                        break;
                    default: // %<x> The literal character represented by <x>
                        builder.appendLiteral(character);
                        break;
                }
                escaped = false;
            }
            else if (character == '%') {
                escaped = true;
            }
            else {
                builder.appendLiteral(character);
            }
        }

        try {
            return builder.toFormatter();
        }
        catch (UnsupportedOperationException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT.toErrorCode(), e);
        }
    }
}
