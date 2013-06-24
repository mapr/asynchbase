/* Copyright (c) 2012 & onwards. MapR Tech, Inc., All rights reserved */
package org.hbase.async;

import static org.apache.hadoop.fs.Path.SEPARATOR;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * <STYLE>table { background #fff; border-collapse: collapse; }
 * th, td { border: 1px #000 solid; padding: 3 }
 * th { background: #eee; }</STYLE>
 *
 *  The <code>TableMappingRule</code> class encapsulate the rules for
 *  deciding if a table name map to HBase table or a MapR table.
 *  <p>Apache HBase has its own flat namespace (no slashes in a table name).
 *  This namespace must be supported by MapR, because:
 *
 *    <ol><li>A customer may run Apache HBase on a MapR cluster</li>
 *    <li>A customer may have applications with hard-coded table
 *       names which they want to migrate to Goose</li></ol>
 *
 *  <p>A single client JAR supports both Goose and Apache HBase.</p>
 *  <p>The following rules determine, based on the table name/path,
 *  whether it's a Goose table or an Apache HBase table:</p>
 *
 *  <p><table border="1" cellpadding="" bordercolor="black"><tbody>
 *  <tr><th align="left">Pattern</th><th>Table type</th></tr>
 *  <tr><td>a/b/c (anything with a slash)&nbsp;&nbsp;&nbsp;</td>
 *  <td align="center">Goose</td></tr>
 *  <tr><td>maprfs://...</td><td align="center">Goose</td></tr>
 *  <tr><td>hbase://mytable1</td><td align="center">Apache HBase</td></tr>
 *  <tr><td>mytable1 (no slash)</td><td align="center">&nbsp;&nbsp;
 *  (See notes below)</td></tr>
 *  </tbody></table></p>
 *
 *  <p>Notes: If a mapping for this table name exists in the client
 *  configuration, this is a Goose table. Otherwise, it is a stock HBase table.
 *  </p>
 *
 *   <p><b><u>HBase table namespace mappings</u></b></p>
 *
 *   <p>As explained in FS-NS-003, there needs to be a way to determine whether
 *   a flat table name (no slashes and doesn't start with hbase://) should be
 *   treated as a stock HBase table or a Goose table. This is accomplished with
 *   a client-side configuration called hbase.table.namespace.mappings.</p>
 *
 *   <b>Example 1:</b>
 *   <p><code>&lt;name>hbase.table.namespace.mappings&lt;/name><br/>
 *    &lt;value>*:/tables&lt;/value></code></p>
 *   <p>Any flat table name foo is treated as a Goose table at /tables/foo.</p>
 *
 *   <b>Example 2:</b>
 *   <p><code>&lt;name>hbase.table.namespace.mappings&lt;/name><br/>
 *   &lt;value>mytable1:/user/aaa,mytable2:/user/bbb&lt;/value></code></p>
 *   <ul><li>The flat table name mytable1 is treated as a Goose table at
 *   /user/aaa/mytable1.</li><li>The flat table name mytable2 is treated as a
 *   Goose table at /user/bbb/mytable2.</li><li>All other flat table names are
 *   treated as stock HBase tables.</li></ul>
 *
 *   <b>Example 3:</b>
 *   <p><code>&lt;name>hbase.table.namespace.mappings&lt;/name><br/>
 *   &lt;value>mytable1:/user/aaa,mapr-*:/user/bbb,*:/tables&lt;/value></code>
 *   </p>
 *   <p>Mappings are evaluated in order. The flat table name <code>mytable1</code>
 *   is treated as a Goose table at <code>/user/aaa/mytable1</code>. Any flat
 *   table with name starting with <code>"mapr-"</code> is treated as a Goose
 *   table at <code>/user/bbb/mapr-*</code>. Any other flat table name
 *   <code>foo</code> is treated as a Goose table at <code>/tables/foo</code>.
 *   </p>
 *
 *   <b>Example 4 (same as not specifying this parameter at all):</b>
 *   <p><code>&lt;name>hbase.table.namespace.mappings&lt;/name><br/>
 *   &lt;value>&lt;/value></code></p>
 *   All flat table names are treated as stock HBase tables.
 */
public class MapRTableMappingRules {
  public static final String MAPR_ENGINE = "mapr";
  public static final String HBASE_ENGINE = "hbase";
  public static final String HBASE_TABLE_NAMESPACE_MAPPINGS =
                                          "hbase.table.namespace.mappings";

  private static final String RULE_SPLITTER = ":";
  private static final String RULE_SEPARATOR = ",";
  private static final String EMPTY_RULE = "";
  private static final Pattern BSLASH_PATTERN = Pattern.compile("\\\\+");

  private static final Logger LOG = LoggerFactory.getLogger(MapRTableMappingRules.class);
  private static final Map<String, RuleList> ruleListMap_ = new HashMap<String, RuleList>();

  private final ClusterType clusterType_;
  private final Configuration conf_;
  private RuleList ruleList_ = null;
  private volatile FileSystem fs_ = null;

  //  Store up to 128 table name to table path mappings
  private Cache<String, Path> namePathCache_ =
      CacheBuilder.newBuilder().maximumSize(128).build();

  public static final String HBASE_AVAILABLE = "hbase.available";
  public static final String MAPRFS_PREFIX = "maprfs://";
  public static final String HBASE_PREFIX = "hbase://";
  public static final Path HBASE_PREFIX_PATH = new Path(HBASE_PREFIX);

  /** The root table's name.*/
  public static final byte [] ROOT_TABLE_NAME = "-ROOT-".getBytes();

  /** The META table's name. */
  public static final byte [] META_TABLE_NAME = ".META.".getBytes();

  /** When we encode strings, we always specify UTF8 encoding */
  public static final String UTF8_ENCODING = "UTF-8";

  /**
   * Describe the type of cluster based on the running services.
   */
  public enum ClusterType {
    /**
     * The cluster runs only HBase service (pre 3.0)
     */
    HBASE_ONLY,
    /**
     * HBase is not installed in the cluster.
     */
    MAPR_ONLY,
    /**
     * The cluster runs both type of DB services.
     */
    HBASE_MAPR
  }

  /**
   * Add the table mapping rule for empty string.
   * Every flat table name is considered to be an HBase table
   */
  static {
    ruleListMap_.put(EMPTY_RULE, new RuleList());
  }

  /**
   * Creates a table mapping rule object based on the value of
   * 'hbase.table.namespace.mappings' parameter in the {@code conf}.
   *
   * <p>The class maintains a global {@link HashMap} of mapping rules string and
   * its compiled form. If the current mapping rule is not found in the map, it
   * is compiled and put in the hash-map.
   *
   * @param conf
   */
  public MapRTableMappingRules(Configuration conf) {
    String mappingRules = conf.get(HBASE_TABLE_NAMESPACE_MAPPINGS, EMPTY_RULE).trim();
    synchronized (ruleListMap_) {
      ruleList_ = ruleListMap_.get(mappingRules);
      if (ruleList_ == null) {
        // compile the rules only once for a given name space mapping string
        ruleList_ = new RuleList(mappingRules);
        ruleListMap_.put(mappingRules, ruleList_);
      }
    }
    conf_ = conf;
    clusterType_ = conf.getBoolean(HBASE_AVAILABLE, true)
        ? ClusterType.HBASE_MAPR : ClusterType.MAPR_ONLY;
  }

  public boolean isMapRDefault() {
    return ruleList_.isAllTableGoose()
        || MAPR_ENGINE.equals(conf_.get("db.engine.default", HBASE_ENGINE));
  }

  public ClusterType getClusterType() {
    return clusterType_;
  }

  /**
   * Tests if <code>tableName</code> should be treated as MapR table
   *
   * @param tableName
   *
   * @return  <code>true</code> if the table is determined to be a MapR table.
   * @throws IOException
   * @throws IllegalArgumentException If the passed {@code tableName} is null
   */
  public boolean isMapRTable(String tableName) {
    return getMapRTablePath(tableName) != null;
  }

  public static boolean isMapRTable(Configuration conf, String tableName) {
    return new MapRTableMappingRules(conf).isMapRTable(tableName);
  }

  public String toString() {
    return "TableMappingRule [rules_='" + ruleList_.rules_ + "']";
  }

  /**
   * Checks of the tableName being passed represents either
   * <code > -ROOT- </code> or <code> .META. </code>
   *
   * @return true if a tablesName is either <code> -ROOT- </code>
   * or <code> .META. </code>
   */
  public static boolean isMetaTable(final String tableName) {
    return Bytes.equals(tableName.getBytes(), ROOT_TABLE_NAME) ||
           Bytes.equals(tableName.getBytes(), META_TABLE_NAME);
  }

  /**
   *  Returns translated path according to the configured mapping.
   *  See {@link MapRTableMappingRules}
   *
   *  @param  tableName Absolute or relative table name
   *
   *  @return Translated absolute path if the table is a MapR table,
   *          <code>null</code> otherwise
   */
  public Path getMapRTablePath(String tableName) throws IllegalArgumentException {
    if (tableName == null) {
      throw new IllegalArgumentException("Table name can not be null.");
    }

    Path tablePath = namePathCache_.getIfPresent(tableName);
    if (tablePath == null) {
      synchronized (namePathCache_) {
        tablePath = namePathCache_.getIfPresent(tableName);
        if (tablePath == null) {
          tablePath = getTablePath(tableName);
          namePathCache_.put(tableName, tablePath);
        }
      }
    }

    return (tablePath != HBASE_PREFIX_PATH) ? tablePath : null;
  }

  private Path getTablePath(String tableName) {
    tableName = BSLASH_PATTERN.matcher(tableName).replaceAll(SEPARATOR);
    if (tableName.startsWith(SEPARATOR)
        || tableName.toLowerCase().startsWith(MAPRFS_PREFIX)) {
      return new Path(tableName);
    }
    else if (tableName.toLowerCase().startsWith(HBASE_PREFIX)
        // If the table name is either .META. or -ROOT-
        || isMetaTable(tableName)) {
      return HBASE_PREFIX_PATH;
    }
    else if (tableName.contains(SEPARATOR)) {
      try {
        return new Path(getFS().getHomeDirectory(), tableName);
      } catch (Exception e) {
        return HBASE_PREFIX_PATH;
      }
    }

    for (Rule rule : ruleList_) {
      String path = rule.uri.toString();
      if (rule.glob.equals(tableName)) {
        return makePath(path, tableName);
      }
      else if (rule.pattern.matcher(tableName).matches()) {
        return makePath((path.endsWith(SEPARATOR) ?
            path : path+SEPARATOR), tableName);
      }
    }
    return HBASE_PREFIX_PATH;
  }

  public static Path getMapRTablePath(Configuration conf, String tableName) {
    return new MapRTableMappingRules(conf).getMapRTablePath(tableName);
  }


  /**
   * Constructs absolute table path from the parent and the table name.
   * <ol><li>If the parent does not end with '/', table name is ignored
   * else table name is appended to parent.<li>If parent does not start
   * with '/', user's home directory is prefixed to the resultant path.
   * </ol>
   *
   * @param parent
   * @param table
   * @return the absolute path
   * @throws IOException
   */
  protected Path makePath(String parent, String table) {
    Path path = parent.endsWith(SEPARATOR) ?
                         new Path(parent, table) : new Path(parent);
    try {
      if (!parent.startsWith(SEPARATOR)) {
        path = new Path(getFS().getHomeDirectory(), path);
      }
    } catch (Exception e) {
      return null;
    }
    return path;
  }

  /**
   * Lazy initialization of {@link FileSystem} object using the {@link Configuration}
   * provided in the constructor. This code will not get executed if there is no
   * rule with relative path.
   *
   * @return the FileSystem object
   * @throws IOException
   */
  protected synchronized FileSystem getFS() throws IOException {
    if (fs_ == null) {
      fs_ = FileSystem.get(conf_);
    }
    return fs_;
  }

  static class RuleList extends ArrayList<MapRTableMappingRules.Rule>{
    private static final long serialVersionUID = 1381209101513814010L;

    private final String rules_;
    private boolean isAllTableGoose_ = false;

    public RuleList() {
      rules_ = EMPTY_RULE;
    }

    RuleList(String rules) {
      rules_ = rules;
      String[] mappings = rules_.split(RULE_SEPARATOR);
      for (int i = 0; i < mappings.length; i++) {
        String mapping = mappings[i].trim().replaceAll("/+", SEPARATOR);
        if (checkAndAdd(mapping)) {
          continue;
        }
        LOG.warn("Invalid mapping '" + mapping + "' found, ignoring.");
      }
    }

    public boolean isAllTableGoose() {
      return isAllTableGoose_;
    }

    public void setAllTableGoose(boolean isAllTableGoose_) {
      this.isAllTableGoose_ = isAllTableGoose_;
    }

    /**
     * Validate the rule syntax and adds the rule to the list if
     * the rule will not be superseded by another rule already in
     * the list before it in which case it logs a warning message.
     *
     * @param mapping    The new rule to add.
     * @return <code>true</code> if the rule syntax is valid,
     *         <code>false</code> otherwise
     */
    protected boolean checkAndAdd(String mapping) {
      int firstSplit = mapping.indexOf(RULE_SPLITTER);
      int lastSplit = mapping.lastIndexOf(RULE_SPLITTER);
      int length = mapping.length();
      if (firstSplit <= 0  // rule contains ':' but not as a first non-whitespace char
          || lastSplit == length-1) { // 'path' is missing
          return false;
      }

      try {
        String[] parts = mapping.split(RULE_SPLITTER, 2);
        String glob = parts[0].trim();
        if (glob.equals("*")) {
          isAllTableGoose_ = true;
        }
        URI uri = new URI(parts[1].trim());
        for(Rule rule: this) {
          if (rule.pattern.matcher(glob).matches()) {
            LOG.warn("Duplicate mapping '" + mapping +
                "' found, ignoring. Original rule '" +
                rule.glob + ":" + rule.uri + "'");
            return true;
          }
        }
        add(new Rule(glob, uri));
        return true;
      }
      catch (Exception e) {
        return false;
      }
    }
  }

  static class Rule {
    final static String REGEX_SPECIAL_CHARS = "[\\^$.|?+()"; // everything except '*'

    final URI uri;
    final String glob;
    final Pattern pattern;

    Rule(final String glob, final URI uri) {
      this.glob = glob;
      this.uri = uri;

      // escape any special regex character,
      // replace '*' with '.*'
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < glob.length(); i++) {
        char ch = glob.charAt(i);
        if (REGEX_SPECIAL_CHARS.indexOf(ch) != -1) {
          sb.append('\\');
        } else if (ch == '*') {
          sb.append('.');
        }
        sb.append(ch);
      }
      this.pattern = Pattern.compile(sb.toString());
    }

    public String toString() {
      return "Rule [pattern=" + glob + ", uri=" + uri + "]";
    }

    boolean matches(String text) {
      return pattern.matcher(text).matches();
    }
  }
}
