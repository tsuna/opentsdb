// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.uid;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

/**
 * Represents a table of Unique IDs, manages the lookup and creation of IDs.
 * <p>
 * Don't attempt to use {@code equals()} or {@code hashCode()} on
 * this class.
 * @see UniqueIdInterface
 */
@SuppressWarnings("deprecation")  // Dunno why even with this, compiler warns.
public final class UniqueId implements UniqueIdInterface {

  private static final Logger LOG = LoggerFactory.getLogger(UniqueId.class);

  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** The single column family used by this class. */
  private static final byte[] ID_FAMILY = toBytes("id");
  /** The single column family used by this class. */
  private static final byte[] NAME_FAMILY = toBytes("name");
  /** Row key of the special row used to track the max ID already assigned. */
  private static final byte[] MAXID_ROW = { 0 };
  /** How many time do we try to assign an ID before giving up. */
  private static final short MAX_ATTEMPTS_ASSIGN_ID = 3;
  /** How many time do we try to apply an edit before giving up. */
  private static final short MAX_ATTEMPTS_PUT = 6;
  /** Initial delay in ms for exponential backoff to retry failed RPCs. */
  private static final short INITIAL_EXP_BACKOFF_DELAY = 800;
  /** Maximum number of results to return in suggest(). */
  private static final short MAX_SUGGESTIONS = 25;

  /** HBase client to use.  */
  private final HBaseClient client;
  /** Table where IDs are stored.  */
  private final byte[] table;
  /** The kind of UniqueId, used as the column qualifier. */
  private final byte[] kind;
  /** Number of bytes on which each ID is encoded. */
  private final short id_width;

  /** Cache for forward mappings (name to ID). */
  private final ConcurrentHashMap<String, byte[]> name_cache =
    new ConcurrentHashMap<String, byte[]>();
  /** Cache for backward mappings (ID to name).
   * The ID in the key is a byte[] converted to a String to be Comparable. */
  private final ConcurrentHashMap<String, String> id_cache =
    new ConcurrentHashMap<String, String>();

  /** Number of times we avoided reading from HBase thanks to the cache. */
  private volatile int cache_hits;
  /** Number of times we had to read from HBase and populate the cache. */
  private volatile int cache_misses;

  /**
   * Constructor.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param kind The kind of Unique ID this instance will deal with.
   * @param width The number of bytes on which Unique IDs should be encoded.
   * @throws IllegalArgumentException if width is negative or too small/large
   * or if kind is an empty string.
   */
  public UniqueId(final HBaseClient client,
                  final byte[] table,
                  final String kind,
                  final int width) {
    this.client = client;
    this.table = table;
    if (kind.isEmpty()) {
      throw new IllegalArgumentException("Empty string as 'kind' argument!");
    }
    this.kind = toBytes(kind);
    if (width < 1 || width > 8) {
      throw new IllegalArgumentException("Invalid width: " + width);
    }
    this.id_width = (short) width;
  }

  /** The number of times we avoided reading from HBase thanks to the cache. */
  public int cacheHits() {
    return cache_hits;
  }

  /** The number of times we had to read from HBase and populate the cache. */
  public int cacheMisses() {
    return cache_misses;
  }

  /** Returns the number of elements stored in the internal cache. */
  public int cacheSize() {
    return name_cache.size() + id_cache.size();
  }

  public String kind() {
    return fromBytes(kind);
  }

  public short width() {
    return id_width;
  }

  /**
   * Causes this instance to discard all its in-memory caches.
   * @since 1.1
   */
  public void dropCaches() {
    name_cache.clear();
    id_cache.clear();
  }

  /**
   * Finds the name associated with a given ID.
   * <p>
   * <strong>This method is blocking.</strong>  Its use within OpenTSDB itself
   * is discouraged, please use {@link #getNameAsync} instead.
   * @param id The ID associated with that name.
   * @see #getId(String)
   * @see #getOrCreateId(String)
   * @throws NoSuchUniqueId if the given ID is not assigned.
   * @throws HBaseException if there is a problem communicating with HBase.
   * @throws IllegalArgumentException if the ID given in argument is encoded
   * on the wrong number of bytes.
   */
  public String getName(final byte[] id) throws NoSuchUniqueId, HBaseException {
    try {
      return getNameAsync(id).joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  /**
   * Finds the name associated with a given ID.
   *
   * @param id The ID associated with that name.
   * @see #getIdAsync(String)
   * @see #getOrCreateIdAsync(String)
   * @throws NoSuchUniqueId if the given ID is not assigned.
   * @throws HBaseException if there is a problem communicating with HBase.
   * @throws IllegalArgumentException if the ID given in argument is encoded
   * on the wrong number of bytes.
   * @since 1.2
   */
  public Deferred<String> getNameAsync(final byte[] id) {
    if (id.length != id_width) {
      throw new IllegalArgumentException("Wrong id.length = " + id.length
                                         + " which is != " + id_width
                                         + " required for '" + kind() + '\'');
    }
    final String name = getNameFromCache(id);
    if (name != null) {
      cache_hits++;
      return Deferred.fromResult(name);
    }
    cache_misses++;
    class GetNameCB implements Callback<String, String> {
      public String call(final String name) {
        if (name == null) {
          throw new NoSuchUniqueId(kind(), id);
        }
        cacheMapping(name, id);
        return name;
      }
    }
    return getNameFromHBase(id).addCallback(new GetNameCB());
  }

  private String getNameFromCache(final byte[] id) {
    return id_cache.get(fromBytes(id));
  }

  private Deferred<String> getNameFromHBase(final byte[] id) {
    class NameFromHBaseCB implements Callback<String, byte[]> {
      public String call(final byte[] name) {
        return name == null ? null : fromBytes(name);
      }
    }
    return hbaseGet(id, NAME_FAMILY).addCallback(new NameFromHBaseCB());
  }

  private void addNameToCache(final byte[] id, final String name) {
    final String key = fromBytes(id);
    String found = id_cache.get(key);
    if (found == null) {
      found = id_cache.putIfAbsent(key, name);
    }
    if (found != null && !found.equals(name)) {
      throw new IllegalStateException("id=" + Arrays.toString(id) + " => name="
          + name + ", already mapped to " + found);
    }
  }

  /**
   * Finds the ID associated with a given name.
   * <p>
   * The length of the byte array is fixed in advance by the implementation.
   * <p>
   * <strong>This method is blocking.</strong>  Its use within OpenTSDB itself
   * is discouraged, please use {@link #getIdAsync} instead.
   *
   * @param name The name to lookup in the table.
   * @see #getName(byte[])
   * @return A non-null, non-empty {@code byte[]} array.
   * @throws NoSuchUniqueName if the name requested doesn't have an ID assigned.
   * @throws HBaseException if there is a problem communicating with HBase.
   * @throws IllegalStateException if the ID found in HBase is encoded on the
   * wrong number of bytes.
   */
  public byte[] getId(final String name) throws NoSuchUniqueName, HBaseException {
    try {
      return getIdAsync(name).joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  /**
   * Finds the ID associated with a given name.
   * <p>
   * The length of the byte array is fixed in advance by the implementation.
   *
   * @param name The name to lookup in the table.
   * @see #getNameAsync(byte[])
   * @return A non-null, non-empty {@code byte[]} array.
   * @throws NoSuchUniqueName if the name requested doesn't have an ID assigned.
   * @throws HBaseException if there is a problem communicating with HBase.
   * @throws IllegalStateException if the ID found in HBase is encoded on the
   * wrong number of bytes.
   * @since 1.2
   */
  public Deferred<byte[]> getIdAsync(final String name) {
    final byte[] id = getIdFromCache(name);
    if (id != null) {
      cache_hits++;
      return Deferred.fromResult(id);
    }
    cache_misses++;
    class GetIdCB implements Callback<byte[], byte[]> {
      public byte[] call(final byte[] id) {
        if (id == null) {
          throw new NoSuchUniqueName(kind(), name);
        }
        if (id.length != id_width) {
          throw new IllegalStateException("Found id.length = " + id.length
                                          + " which is != " + id_width
                                          + " required for '" + kind() + '\'');
        }
        cacheMapping(name, id);
        return id;
      }
    }
    Deferred<byte[]> d = getIdFromHBase(name).addCallback(new GetIdCB());
    return d;
  }

  private byte[] getIdFromCache(final String name) {
    return name_cache.get(name);
  }

  private Deferred<byte[]> getIdFromHBase(final String name) {
    return hbaseGet(toBytes(name), ID_FAMILY);
  }

  private void addIdToCache(final String name, final byte[] id) {
    byte[] found = name_cache.get(name);
    if (found == null) {
      found = name_cache.putIfAbsent(name,
                                    // Must make a defensive copy to be immune
                                    // to any changes the caller may do on the
                                    // array later on.
                                    Arrays.copyOf(id, id.length));
    }
    if (found != null && !Arrays.equals(found, id)) {
      throw new IllegalStateException("name=" + name + " => id="
          + Arrays.toString(id) + ", already mapped to "
          + Arrays.toString(found));
    }
  }

  /** Adds the bidirectional mapping in the cache.  */
  private void cacheMapping(final String name, final byte[] id) {
    addIdToCache(name, id);
    addNameToCache(id, name);
  }

  /**
   * Implements the process to allocate a new UID.
   * This callback is re-used multiple times in a four step process:
   *   1. Allocate a new UID via atomic increment.
   *   2. Create the reverse mapping (ID to name).
   *   3. Create the forward mapping (name to ID).
   *   4. Return the new UID to the caller.
   */
  private final class UniqueIdAllocator implements Callback<Object, Object> {
    private final String name;  // What we're trying to allocate an ID for.
    private short attempt = MAX_ATTEMPTS_ASSIGN_ID;  // Give up when zero.

    private HBaseException hbe = null;  // Last exception caught.

    private long id = -1;  // The ID we'll grab with an atomic increment.
    private byte row[];    // The same ID, as a byte array.

    private static final byte ALLOCATE_UID = 0;
    private static final byte CREATE_REVERSE_MAPPING = 1;
    private static final byte CREATE_FORWARD_MAPPING = 2;
    private static final byte DONE = 3;
    private byte state = ALLOCATE_UID;  // Current state of the process.

    UniqueIdAllocator(final String name) {
      this.name = name;
    }

    @SuppressWarnings("unchecked")
    Deferred<byte[]> tryAllocate() {
      attempt--;
      state = ALLOCATE_UID;
      return (Deferred<byte[]>) call(null);
    }

    @SuppressWarnings("unchecked")
    public Object call(final Object arg) {
      if (attempt == 0) {
        if (hbe == null) {
          throw new IllegalStateException("Should never happen!");
        }
        LOG.error("Failed to assign an ID for kind='" + kind()
                  + "' name='" + name + "'", hbe);
        throw hbe;
      }

      if (arg instanceof Exception) {
        final String msg = ("Failed attempt #" + (MAX_ATTEMPTS_ASSIGN_ID - attempt)
                            + " to assign an UID for " + kind() + ':' + name
                            + " at step #" + state);
        if (arg instanceof HBaseException) {
          LOG.error(msg, (Exception) arg);
          hbe = (HBaseException) arg;
          return tryAllocate();  // Retry from the beginning.
        } else {
          LOG.error("WTF?  Unexpected exception!  " + msg, (Exception) arg);
          return arg;  // Unexpected exception, let it bubble up.
        }
      }

      final Deferred d;
      switch (state) {
        case ALLOCATE_UID:
          d = allocateUid();
          break;
        case CREATE_REVERSE_MAPPING:
          d = createReverseMapping(arg);
          break;
        case CREATE_FORWARD_MAPPING:
          d = createForwardMapping(arg);
          break;
        case DONE:
          return done(arg);
        default:
          throw new AssertionError("Should never be here!");
      }
      return d.addBoth(this);
    }

    private Deferred<Long> allocateUid() {
      LOG.info("Creating an ID for kind='" + kind()
               + "' name='" + name + '\'');

      state = CREATE_REVERSE_MAPPING;
      return client.atomicIncrement(new AtomicIncrementRequest(table, MAXID_ROW,
                                                               ID_FAMILY,
                                                               kind));
    }


    /**
     * Create the reverse mapping.
     * We do this before the forward one so that if we die before creating
     * the forward mapping we don't run the risk of "publishing" a
     * partially assigned ID.  The reverse mapping on its own is harmless
     * but the forward mapping without reverse mapping is bad as it would
     * point to an ID that cannot be resolved.
     */
    private Deferred<Boolean> createReverseMapping(final Object arg) {
      if (!(arg instanceof Long)) {
        throw new IllegalStateException("Expected a Long but got " + arg);
      }
      id = (Long) arg;
      if (id <= 0) {
        throw new IllegalStateException("Got a negative ID from HBase: " + id);
      }
      LOG.info("Got ID=" + id
               + " for kind='" + kind() + "' name='" + name + "'");
      row = Bytes.fromLong(id);
      // row.length should actually be 8.
      if (row.length < id_width) {
        throw new IllegalStateException("OMG, row.length = " + row.length
                                        + " which is less than " + id_width
                                        + " for id=" + id
                                        + " row=" + Arrays.toString(row));
      }
      // Verify that we're going to drop bytes that are 0.
      for (int i = 0; i < row.length - id_width; i++) {
        if (row[i] != 0) {
          final String message = "All Unique IDs for " + kind()
            + " on " + id_width + " bytes are already assigned!";
          LOG.error("OMG " + message);
          throw new IllegalStateException(message);
        }
      }
      // Shrink the ID on the requested number of bytes.
      row = Arrays.copyOfRange(row, row.length - id_width, row.length);

      state = CREATE_FORWARD_MAPPING;
      // We are CAS'ing the KV into existence -- the second argument is how
      // we tell HBase we want to atomically create the KV, so that if there
      // is already a KV in this cell, we'll fail.  Technically we could do
      // just a `put' here, as we have a freshly allocated UID, so there is
      // not reason why a KV should already exist for this UID, but just to
      // err on the safe side and catch really weird corruption cases, we do
      // a CAS instead to create the KV.
      return client.compareAndSet(reverseMapping(), HBaseClient.EMPTY_ARRAY);
    }

    private PutRequest reverseMapping() {
      return new PutRequest(table, row, NAME_FAMILY, kind, toBytes(name));
    }

    private Deferred<?> createForwardMapping(final Object arg) {
      if (!(arg instanceof Boolean)) {
        throw new IllegalStateException("Expected a Boolean but got " + arg);
      }
      if (!((Boolean) arg)) {  // Previous CAS failed.  Something is really messed up.
        LOG.error("WTF!  Failed to CAS reverse mapping: " + reverseMapping()
                  + " -- run an fsck against the UID table!");
        return tryAllocate();  // Try again from the beginning.
      }

      state = DONE;
      return client.compareAndSet(forwardMapping(), HBaseClient.EMPTY_ARRAY);
    }

    private PutRequest forwardMapping() {
        return new PutRequest(table, toBytes(name), ID_FAMILY, kind, row);
    }

    private Deferred<byte[]> done(final Object arg) {
      if (!(arg instanceof Boolean)) {
        throw new IllegalStateException("Expected a Boolean but got " + arg);
      }
      if (!((Boolean) arg)) {  // Previous CAS failed.  We lost a race.
        LOG.warn("Race condition: tried to assign ID " + id + " to "
                 + kind() + ":" + name + ", but CAS failed on "
                 + forwardMapping() + ", which indicates this UID must have"
                 + " been allocated concurrently by another TSD. So ID "
                 + id + " was leaked.");
        // If two TSDs attempted to allocate a UID for the same name at the
        // same time, they would both have allocated a UID, and created a
        // reverse mapping, and upon getting here, only one of them would
        // manage to CAS this KV into existence.  The one that loses the
        // race will retry and discover the UID assigned by the winner TSD,
        // and a UID will have been wasted in the process.  No big deal.
        return getIdAsync(name);
      }

      cacheMapping(name, row);
      return Deferred.fromResult(row);
    }

  }


  /**
   * Finds the ID associated with a given name or creates it.
   * <p>
   * <strong>This method is blocking.</strong>  Its use within OpenTSDB itself
   * is discouraged, please use {@link #getOrCreateIdAsync} instead.
   * <p>
   * The length of the byte array is fixed in advance by the implementation.
   *
   * @param name The name to lookup in the table or to assign an ID to.
   * @throws HBaseException if there is a problem communicating with HBase.
   * @throws IllegalStateException if all possible IDs are already assigned.
   * @throws IllegalStateException if the ID found in HBase is encoded on the
   * wrong number of bytes.
   */
  public byte[] getOrCreateId(final String name) throws HBaseException {
    try {
      return getOrCreateIdAsync(name).joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  /**
   * Finds the ID associated with a given name or creates it.
   * <p>
   * The length of the byte array is fixed in advance by the implementation.
   *
   * @param name The name to lookup in the table or to assign an ID to.
   * @throws HBaseException if there is a problem communicating with HBase.
   * @throws IllegalStateException if all possible IDs are already assigned.
   * @throws IllegalStateException if the ID found in HBase is encoded on the
   * wrong number of bytes.
   * @since 1.2
   */
  public Deferred<byte[]> getOrCreateIdAsync(final String name) {
    // Look in the cache first.
    final byte[] id = getIdFromCache(name);
    if (id != null) {
      cache_hits++;
      return Deferred.fromResult(id);
    }
    // Not found in our cache, so look in HBase instead.

    class HandleNoSuchUniqueNameCB implements Callback<Object, Exception> {
      public Object call(final Exception e) {
        if (e instanceof NoSuchUniqueName) {
          return new UniqueIdAllocator(name).tryAllocate();
        }
        return e;  // Other unexpected exception, let it bubble up.
      }
    }

    // Kick off the HBase lookup, and if we don't find it there either, start
    // the process to allocate a UID.
    return getIdAsync(name).addErrback(new HandleNoSuchUniqueNameCB());
  }

  /**
   * Attempts to find suggestions of names given a search term.
   * <p>
   * <strong>This method is blocking.</strong>  Its use within OpenTSDB itself
   * is discouraged, please use {@link #suggestAsync} instead.
   * @param search The search term (possibly empty).
   * @return A list of known valid names that have UIDs that sort of match
   * the search term.  If the search term is empty, returns the first few
   * terms.
   * @throws HBaseException if there was a problem getting suggestions from
   * HBase.
   */
  public List<String> suggest(final String search) throws HBaseException {
    try {
      return suggestAsync(search).joinUninterruptibly();
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {  // Should never happen.
      final String msg = "Unexpected exception caught by "
        + this + ".suggest(" + search + ')';
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);  // Should never happen.
    }
  }

  /**
   * Attempts to find suggestions of names given a search term.
   * @param search The search term (possibly empty).
   * @return A list of known valid names that have UIDs that sort of match
   * the search term.  If the search term is empty, returns the first few
   * terms.
   * @throws HBaseException if there was a problem getting suggestions from
   * HBase.
   * @since 1.2
   */
  public Deferred<List<String>> suggestAsync(final String search) {
    return new SuggestCB(search).search();
  }

  /**
   * Helper callback to asynchronously scan HBase for suggestions.
   */
  private final class SuggestCB
    implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
    private final LinkedList<String> suggestions = new LinkedList<String>();
    private final Scanner scanner;

    SuggestCB(final String search) {
      this.scanner = getSuggestScanner(search);
    }

    @SuppressWarnings("unchecked")
    Deferred<List<String>> search() {
      return (Deferred) scanner.nextRows().addCallback(this);
    }

    public Object call(final ArrayList<ArrayList<KeyValue>> rows) {
      if (rows == null) {  // We're done scanning.
        return suggestions;
      } else {
        for (final ArrayList<KeyValue> row : rows) {
          if (row.size() != 1) {
            LOG.error("WTF shouldn't happen!  Scanner " + scanner + " returned"
                      + " a row that doesn't have exactly 1 KeyValue: " + row);
            if (row.isEmpty()) {
              continue;
            }
          }
          final byte[] key = row.get(0).key();
          final String name = fromBytes(key);
          final byte[] id = row.get(0).value();
          final byte[] cached_id = name_cache.get(name);
          if (cached_id == null) {
            cacheMapping(name, id);
          } else if (!Arrays.equals(id, cached_id)) {
            throw new IllegalStateException("WTF?  For kind=" + kind()
              + " name=" + name + ", we have id=" + Arrays.toString(cached_id)
              + " in cache, but just scanned id=" + Arrays.toString(id));
          }
          suggestions.add(name);
          if ((short) suggestions.size() >= MAX_SUGGESTIONS) {
            scanner.close();
            return suggestions;  // We have enough.
          }
          row.clear();  // free()
        }
      }
      return search();  // Get more suggestions.
    }
  }

  /**
   * Reassigns the UID to a different name (non-atomic).
   * <p>
   * Whatever was the UID of {@code oldname} will be given to {@code newname}.
   * {@code oldname} will no longer be assigned a UID.
   * <p>
   * Beware that the assignment change is <b>not atommic</b>.  If two threads
   * or processes attempt to rename the same UID differently, the result is
   * unspecified and might even be inconsistent.  This API is only here for
   * administrative purposes, not for normal programmatic interactions.
   * @param oldname The old name to rename.
   * @param newname The new name.
   * @throws NoSuchUniqueName if {@code oldname} wasn't assigned.
   * @throws IllegalArgumentException if {@code newname} was already assigned.
   * @throws HBaseException if there was a problem with HBase while trying to
   * update the mapping.
   */
  public void rename(final String oldname, final String newname) {
    final byte[] row = getId(oldname);
    {
      byte[] id = null;
      try {
        id = getId(newname);
      } catch (NoSuchUniqueName e) {
        // OK, we don't want the new name to be assigned.
      }
      if (id != null) {
        throw new IllegalArgumentException("When trying rename(\"" + oldname
          + "\", \"" + newname + "\") on " + this + ": new name already"
          + " assigned ID=" + Arrays.toString(id));
      }
    }

    final byte[] newnameb = toBytes(newname);

    // Update the reverse mapping first, so that if we die before updating
    // the forward mapping we don't run the risk of "publishing" a
    // partially assigned ID.  The reverse mapping on its own is harmless
    // but the forward mapping without reverse mapping is bad.
    try {
      final PutRequest reverse_mapping = new PutRequest(
        table, row, NAME_FAMILY, kind, newnameb);
      hbasePutWithRetry(reverse_mapping, MAX_ATTEMPTS_PUT,
                        INITIAL_EXP_BACKOFF_DELAY);
    } catch (HBaseException e) {
      LOG.error("When trying rename(\"" + oldname
        + "\", \"" + newname + "\") on " + this + ": Failed to update reverse"
        + " mapping for ID=" + Arrays.toString(row), e);
      throw e;
    }

    // Now create the new forward mapping.
    try {
      final PutRequest forward_mapping = new PutRequest(
        table, newnameb, ID_FAMILY, kind, row);
      hbasePutWithRetry(forward_mapping, MAX_ATTEMPTS_PUT,
                        INITIAL_EXP_BACKOFF_DELAY);
    } catch (HBaseException e) {
      LOG.error("When trying rename(\"" + oldname
        + "\", \"" + newname + "\") on " + this + ": Failed to create the"
        + " new forward mapping with ID=" + Arrays.toString(row), e);
      throw e;
    }

    // Update cache.
    addIdToCache(newname, row);            // add     new name -> ID
    id_cache.put(fromBytes(row), newname);  // update  ID -> new name
    name_cache.remove(oldname);             // remove  old name -> ID

    // Delete the old forward mapping.
    try {
      final DeleteRequest old_forward_mapping = new DeleteRequest(
        table, toBytes(oldname), ID_FAMILY, kind);
      client.delete(old_forward_mapping).joinUninterruptibly();
    } catch (HBaseException e) {
      LOG.error("When trying rename(\"" + oldname
        + "\", \"" + newname + "\") on " + this + ": Failed to remove the"
        + " old forward mapping for ID=" + Arrays.toString(row), e);
      throw e;
    } catch (Exception e) {
      final String msg = "Unexpected exception when trying rename(\"" + oldname
        + "\", \"" + newname + "\") on " + this + ": Failed to remove the"
        + " old forward mapping for ID=" + Arrays.toString(row);
      LOG.error("WTF?  " + msg, e);
      throw new RuntimeException(msg, e);
    }
    // Success!
  }

  /** The start row to scan on empty search strings.  `!' = first ASCII char. */
  private static final byte[] START_ROW = new byte[] { '!' };

  /** The end row to scan on empty search strings.  `~' = last ASCII char. */
  private static final byte[] END_ROW = new byte[] { '~' };

  /**
   * Creates a scanner that scans the right range of rows for suggestions.
   */
  private Scanner getSuggestScanner(final String search) {
    final byte[] start_row;
    final byte[] end_row;
    if (search.isEmpty()) {
      start_row = START_ROW;
      end_row = END_ROW;
    } else {
      start_row = toBytes(search);
      end_row = Arrays.copyOf(start_row, start_row.length);
      end_row[start_row.length - 1]++;
    }
    final Scanner scanner = client.newScanner(table);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    scanner.setFamily(ID_FAMILY);
    scanner.setQualifier(kind);
    scanner.setMaxNumRows(MAX_SUGGESTIONS);
    return scanner;
  }

  /** Returns the cell of the specified row key, using family:kind. */
  private Deferred<byte[]> hbaseGet(final byte[] key, final byte[] family) {
    final GetRequest get = new GetRequest(table, key);
    get.family(family).qualifier(kind);
    class GetCB implements Callback<byte[], ArrayList<KeyValue>> {
      public byte[] call(final ArrayList<KeyValue> row) {
        if (row == null || row.isEmpty()) {
          return null;
        }
        return row.get(0).value();
      }
    }
    return client.get(get).addCallback(new GetCB());
  }

  /**
   * Attempts to run the PutRequest given in argument, retrying if needed.
   *
   * Puts are synchronized.
   *
   * @param put The PutRequest to execute.
   * @param attempts The maximum number of attempts.
   * @param wait The initial amount of time in ms to sleep for after a
   * failure.  This amount is doubled after each failed attempt.
   * @throws HBaseException if all the attempts have failed.  This exception
   * will be the exception of the last attempt.
   */
  private void hbasePutWithRetry(final PutRequest put, short attempts, short wait)
    throws HBaseException {
    put.setBufferable(false);  // TODO(tsuna): Remove once this code is async.
    while (attempts-- > 0) {
      try {
        client.put(put).joinUninterruptibly();
        return;
      } catch (HBaseException e) {
        if (attempts > 0) {
          LOG.error("Put failed, attempts left=" + attempts
                    + " (retrying in " + wait + " ms), put=" + put, e);
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ie) {
            throw new RuntimeException("interrupted", ie);
          }
          wait *= 2;
        } else {
          throw e;
        }
      } catch (Exception e) {
        LOG.error("WTF?  Unexpected exception type, put=" + put, e);
      }
    }
    throw new IllegalStateException("This code should never be reached!");
  }

  private static byte[] toBytes(final String s) {
    return s.getBytes(CHARSET);
  }

  private static String fromBytes(final byte[] b) {
    return new String(b, CHARSET);
  }

  /** Returns a human readable string representation of the object. */
  public String toString() {
    return "UniqueId(" + fromBytes(table) + ", " + kind() + ", " + id_width + ")";
  }

}
