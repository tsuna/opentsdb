// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.graph;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.Tags;

/**
 * Data points loaded from a Gnuplot data file.
 * <p>
 * This class is used to re-load data points that were previously fetched
 * from HBase and sent to Gnuplot.
 */
final class GnuplotDataPoints implements DataPoints {

  private static final Logger LOG = LoggerFactory.getLogger(GnuplotDataPoints.class);

  /** Name of the metric.  */
  private final String metric;

  /** Tags (map from tag name to tag value).  */
  private final HashMap<String, String> tags;

  /** How many data points do we have?  */
  private int size;

  /**
   * Timestamps for individual data points.
   */
  private final int[] timestamps;

  /**
   * Types of the data points (floating point vs integer).
   */
  private final boolean[] is_int;

  /**
   * Each value.
   */
  private final long[] values;

  /**
   * Constructor.
   * @param metric The name of the metric.
   * @param tags The tags on these data points.
   * @param size The number of data points in this instance.
   * @param timestamps The timestamps for each data point.
   * @param is_int The type for each data point (floating point vs integer).
   * @param values The values for each data point.
   */
  private GnuplotDataPoints(final String metric,
                            final HashMap<String, String> tags,
                            final int size,
                            final int[] timestamps,
                            final boolean[] is_int,
                            final long[] values) {
    if (timestamps.length != is_int.length
        || timestamps.length != values.length
        || timestamps.length < size) {
      throw new AssertionError("Invalid arguments: metric=" + metric
        + ", tags=" + tags + ", size=" + size
        + ", #timestamps=" + timestamps.length
        + ", #is_int=" + is_int.length + ", #values=" + values.length);
    }
    this.metric = metric;
    this.tags = tags;
    this.size = size;
    this.timestamps = timestamps;
    this.is_int = is_int;
    this.values = values;
  }

  /**
   * Loads data points from the given Gnuplot data file.
   * <p>
   * This is useful to reload data points we previous fed to Gnuplot without
   * scanning HBase again, because it's faster (sometimes an order of
   * magnitude faster, or even more).
   * @param path The path from which to load the data points.
   * @param utc_offset The offset in seconds from UTC to apply to all the
   * timestamps we read.
   * @return The data points, loaded in memory.
   * @throws IOException if there was a problem while opening or reading or
   * closing the given path.
   * @throws FileNotFoundException if the given path doesn't exist.
   * @throws IllegalArgumentException if the path contains malformed data.
   */
  static GnuplotDataPoints fromFile(final String path,
                                    final int utc_offset) throws IOException {
    final GnuplotDataPoints[] out = new GnuplotDataPoints[1];
    loadFile(path, out, utc_offset, null, -1);
    return out[0];
  }

  /**
   * Loads the title and number of data points from the given data file.
   * <p>
   * This simply inspects the file to extract the "title" (metric + tags) of
   * the data points stored in this file, and counts the number of data
   * points stored in the file without retaining them in memory.
   * @param path The path from which to load the title and count data points.
   * @param titles The array into which to store the title found.
   * @param title_index The index in the array where to store the title.
   * @return The number of data points stored in the given path.
   * @throws IOException if there was a problem while opening or reading or
   * closing the given path.
   * @throws FileNotFoundException if the given path doesn't exist.
   */
  static int loadTitleAndCount(final String path,
                               final String[] titles,
                               final int title_index) throws IOException {
    return loadFile(path, null, 0, titles, title_index);
  }

  /**
   * Helper to load or count data points from the given Gnuplot data file.
   * @param path The path from which to load the title and count data points.
   * @param out Used as second return value if not null: the data points
   * will be retained in memory and returned via the first element of this
   * array.
   * @param utc_offset The offset in seconds from UTC to apply to all the
   * timestamps we read.  Ignore if `out' is null.
   * @param titles The array into which to store the title found.  Ignored
   * if null.
   * @param title_index The index in the array where to store the title.
   * @return The number of data points stored in the given path.
   * @throws IOException if there was a problem while opening or reading or
   * closing the given path.
   * @throws FileNotFoundException if the given path doesn't exist.
   * @throws IllegalArgumentException if the path contains malformed data.
   */
  private static int loadFile(final String path,
                              final GnuplotDataPoints[] out,
                              final int utc_offset,
                              final String[] titles,
                              final int title_index) throws IOException {
    final FileInputStream in = new FileInputStream(path);
    String title = "??";
    try {
      int n = (int) new File(path).length();
      if (n <= 0) {
        LOG.warn("File seems empty: " + path);
        throw new FileNotFoundException(path);
      }
      final byte[] buf = new byte[Math.min(n, 2048)];
      int read;  // How many bytes have we read into `buf'?

      // The first `buf' we read should contain a special line
      // with the title of the time series we're plotting.  So
      // handle it specially.  E.g. the first line should be:
      // "# metric{tag=value}" -- note that we don't use the JRE's
      // readLine because its implementation is typically fucking
      // retarded (see su.pr/3gZ2tg) and is over 300x slower!!!!11one
      if ((read = in.read(buf)) <= 0) {
        LOG.warn("Couldn't read anything from " + path);
        return 0;
      }
      int i = 0;  // Index in `buf'.
      if (buf[0] == '#' && buf[1] == ' ') {
        for (i = 2; i < read; i++) {
          if (buf[i] == '\n') {
            title = new String(buf, 2, i - 2);
            i++;  // Get past the '\n' we've just found.
            break;
          }
        }
      } else {
        LOG.warn("Couldn't find a title in " + path);
      }

      // Now go through every line remaining in the file.  If `out' is null,
      // just count the lines, otherwise load each data point from each line.
      // The format of each line is straightforward: "$timestamp $value\n".
      // Values can be integers (123) or floating point (1.23 or 1.2E3).
      // Assume a conservative average characters per line to pre-size arrays.
      int[] timestamps = out == null ? null : new int[Math.max(n / 29, 10)];
      boolean[] is_int = out == null ? null : new boolean[timestamps.length];
      long[] values = out == null ? null : new long[timestamps.length];
      if (out != null) {
        LOG.info("Start with size = " + timestamps.length);
      }
      n = 0;  // Current index in the arrays above.

      long ts = 0;             // Current timestamp.
      final StringBuilder val = out == null ? null : new StringBuilder(18);
      boolean readts = true;   // What are we reading?  true ? ts : val.
      boolean readint = true;  // Does val contain a long or a float?
      do {
        for (/**/; i < read; i++) {
          final byte b = buf[i];
          if (b == '\n') {
            if (val != null) {
              if (readts || val.length() == 0) {
                throw new IllegalArgumentException("Line #" + (n + 1)
                  + " in " + path + " is malformed (no value).");
              }
              if (n == timestamps.length) {  // Need to grow the arrays.
                // Because our array should already be pretty close to the
                // right size, only increase it size by 100 entries.
                timestamps = Arrays.copyOf(timestamps, n + 100);
                /*XXX*/LOG.info("Grow to    size = " + timestamps.length);
                is_int = Arrays.copyOf(is_int, timestamps.length);
                values = Arrays.copyOf(values, timestamps.length);
              }
              timestamps[n] = (int) (ts - utc_offset);
              if (readint) {
                values[n] = Tags.parseLong(val);
                is_int[n] = true;
              } else {
                final double f = Double.parseDouble(val.toString());
                values[n] = Double.doubleToRawLongBits(f);
                is_int[n] = false;
              }
              val.setLength(0);
              readts = true;  // End of line, start reading a new ts.
              readint = true;
              ts = 0;
            }
            n++;
            continue;
          } else if (val == null) {
            continue;  // We're only counting lines.
          }
          switch (b) {
            case ' ':
              if (!readts || ts == 0) {
                throw new IllegalArgumentException("Line #" + (n + 1)
                  + " in " + path + " is malformed (no timestamp).");
              }
              readts = false;
              break;
            case '.':
            case 'E':
              readint = false;
              val.append((char) b);
              break;
            default:
              if (readts) {
                if ('0' <= b && b <= '9') {
                  ts *= 10;
                  ts += b - '0';
                } else {
                  throw new IllegalArgumentException("Line #" + (n + 1)
                    + " in " + path + " is malformed ('" + b + "' in ts).");
                }
              } else {
                val.append((char) b);
              }
              break;
          }
        }
        i = 0;
      } while ((read = in.read(buf)) > 0);

      if (out != null) {
        final HashMap<String, String> tags = new HashMap<String, String>();
        final String metric = Tags.parseWithMetric(title, tags);
        final GnuplotDataPoints dp =
          new GnuplotDataPoints(metric, tags, n, timestamps, is_int, values);
        out[0] = dp;
      }
      return n;
    } finally {
      in.close();
      if (titles != null) {
        titles[title_index] = title;
      }
    }
  }

  public String metricName() {
    return metric;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public List<String> getAggregatedTags() {
    return Collections.emptyList();
  }

  public int size() {
    return size;
  }

  public int aggregatedSize() {
    return 0;
  }

  public SeekableView iterator() {
    return new View();
  }

  /** @throws IndexOutOfBoundsException if {@code i} is out of bounds. */
  private void checkIndex(final int i) {
    if (i >= size()) {
      throw new IndexOutOfBoundsException("index " + i + " >= " + size()
          + " for this=" + this);
    }
    if (i < 0) {
      throw new IndexOutOfBoundsException("negative index " + i
          + " for this=" + this);
    }
  }

  public long timestamp(final int i) {
    checkIndex(i);
    return timestamps[i];
  }

  public boolean isInteger(final int i) {
    checkIndex(i);
    return is_int[i];
  }

  public long longValue(final int i) {
    // Don't call checkIndex(i) because isInteger(i) already calls it.
    if (isInteger(i)) {
      return values[i];
    }
    throw new ClassCastException("value #" + i + " is not a long in " + this);
  }

  public double doubleValue(final int i) {
    // Don't call checkIndex(i) because isInteger(i) already calls it.
    if (!isInteger(i)) {
      return Double.longBitsToDouble(values[i]);
    }
    throw new ClassCastException("value #" + i + " is not a float in " + this);
  }

  public long getHash() {
    long hash = ((long) metric.hashCode()) << 32;
    hash ^= tags.hashCode() * 7L;
    hash ^= timestamps[0] * 31L;
    hash ^= timestamps[timestamps.length - 1] * 7L;
    return hash;
  }

  /** Returns a human readable string representation of the object. */
  public String toString() {
    // The argument passed to StringBuilder is a pretty good estimate of the
    // length of the final string.
    final StringBuilder buf = new StringBuilder(80 + metric.length()
                                                + tags.size() * 16
                                                + size * 16);
    buf.append("GnuplotDataPoints(metric=")
       .append(metric)
       .append(", tags=")
       .append(tags)
       .append(", [");
    for (int i = 0; i < size; i++) {
      buf.append(timestamps[i]);
      if (isInteger(i)) {
        buf.append(":long(").append(longValue(i));
      } else {
        buf.append(":float(").append(doubleValue(i));
      }
      buf.append(')');
      if (i != size - 1) {
        buf.append(", ");
      }
    }
    buf.append("])");
    return buf.toString();
  }

  /**
   * View/iterator for this class of data points.
   */
  private final class View implements SeekableView, DataPoint {

    /** Where are we in the iteration.  */
    private int index = -1;

    // ------------------ //
    // Iterator interface //
    // ------------------ //

    public boolean hasNext() {
      return index < size - 1;
    }

    public DataPoint next() {
      if (hasNext()) {
        index++;
        return this;
      }
      throw new NoSuchElementException("no more elements in " + this);
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    // ---------------------- //
    // SeekableView interface //
    // ---------------------- //

    public void seek(final long timestamp) {
      if ((timestamp & 0xFFFFFFFF00000000L) != 0) {  // negative or not 32 bits
        throw new IllegalArgumentException("invalid timestamp: " + timestamp);
      }
      index = Arrays.binarySearch(timestamps, (int) timestamp);
      if (index >= 0) {  // Found exact match.
        index--;        // So that next() returns the exact match first.
      } else {  // Exact match not found.
        index = -(index + 1);
      }
    }

    // ------------------- //
    // DataPoint interface //
    // ------------------- //

    public long timestamp() {
      return GnuplotDataPoints.this.timestamp(index);
    }

    public boolean isInteger() {
      return GnuplotDataPoints.this.isInteger(index);
    }

    public long longValue() {
      return GnuplotDataPoints.this.longValue(index);
    }

    public double doubleValue() {
      return GnuplotDataPoints.this.doubleValue(index);
    }

    public double toDouble() {
      return isInteger() ? longValue() : doubleValue();
    }

    public String toString() {
      return "View(index=" + index + ", " + GnuplotDataPoints.this.toString() + ')';
    }

  }

}
