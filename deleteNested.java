
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.io.*;

public class deleteNested {
	static String infoSchemaName = "information_schema";
	static String remoteSchemaName = "sakila"; // CONFIGURABLE BY USER
	static String remotePort = "3306"; // CONFIGURABLE BY USER
	static String remoteUserName = "root"; // CONFIGURABLE BY USER
	static String remotePassword = "root"; // CONFIGURABLE BY USER

	public static void main(String[] args) throws FileNotFoundException {

		sqlConnection(remoteSchemaName, infoSchemaName, remotePort, remoteUserName, remotePassword);
	}

	static void deleteQueries(String remoteSchemaName, String infoSchemaName, Connection remoteConnection,
			Connection infoConnection) {

		String tableToProcess = "address"; // CONFIGURABLE BY USER
		String dataToDelete = "3"; // CONFIGURABLE BY USER

		startFn(remoteConnection, tableToProcess, dataToDelete);

	}

	static void startFn(Connection remoteConnection, String tableToProcess, String dataToDelete) {
		PrintStream o;
		try {
			o = new PrintStream(new File("A.txt"));
			PrintStream console = System.out;

			MultiMap<String, String> ListOfParentTablesMap = new MultiMap();
			MultiMap<String, String> ListOfInnerTablesWithDataMap = new MultiMap();

			ListOfParentTablesMap = ListOfParentTables(remoteConnection, tableToProcess);
			if (ListOfParentTablesMap.size() > 0) {
				ListOfInnerTablesWithDataMap = ListOfInnerTablesWithData(remoteConnection, ListOfParentTablesMap,
						dataToDelete);
				if (ListOfInnerTablesWithDataMap.size() > 0) {
					for (String i : ListOfInnerTablesWithDataMap.keySet()) {
						for (String ii : ListOfInnerTablesWithDataMap.get(i)) {
							System.setOut(o);
							System.out.println(i + " " + ii);

							System.setOut(console);
							startFn(remoteConnection, i, ii);
						}
					}
				}

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	static MultiMap<String, String> ListOfInnerTablesWithData(Connection remoteConnection,
			MultiMap<String, String> multimap, String dataToDelete) {
		Statement stmt;
		MultiMap<String, String> tempmap2 = new MultiMap();
		try {
			stmt = remoteConnection.createStatement();
			for (String i : multimap.keySet()) {
				for (String Colname : multimap.get(i)) {
					String sql = "select count(*) from " + i + " where " + Colname + " IN(" + dataToDelete + ");";
					ResultSet rs;

					rs = stmt.executeQuery(sql);

					while (rs.next()) {
						if (rs.getInt(1) > 0) {
//							System.out.println(i + " " + rs.getInt(1));
//						multimap1=ListOfParentTables(remoteConnection, i);
						}
					}
					String sql2 = "select * from " + i + " where " + Colname + " IN(" + dataToDelete + ");";
					ResultSet rs2 = stmt.executeQuery(sql2);
					while (rs2.next()) {
						tempmap2.put(i, rs2.getString(1));
//						System.out.println(rs2.getInt(1));
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tempmap2;
	}

	static MultiMap<String, String> ListOfParentTables(Connection remoteConnection, String tableToProcess) {
		PreparedStatement ps;
		int count = 0;
		MultiMap<String, String> multimap = new MultiMap();
		try {
			ps = remoteConnection.prepareStatement(
					"SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA = ? AND "
							+ "REFERENCED_TABLE_NAME = ?;");
			ps.setString(1, remoteSchemaName);
			ps.setString(2, tableToProcess);
			ResultSet result = ps.executeQuery();
			while (result.next()) {
				multimap.put(result.getString(1), result.getString(2));
				count++;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return multimap;
	}

	static void sqlConnection(String remoteSchemaName, String infoSchemaName, String remotePort, String remoteUserName,
			String remotePassword) {
		try {

			Connection remoteConnection = DriverManager.getConnection(
					"jdbc:mysql://localhost:" + remotePort + "/" + remoteSchemaName, remoteUserName, remotePassword);
			Connection infoConnection = DriverManager.getConnection(
					"jdbc:mysql://localhost:" + remotePort + "/" + infoSchemaName, remoteUserName, remotePassword);
			System.out.println("ALL CONNECTIONS ESTABLISHED");
			deleteQueries(remoteSchemaName, infoSchemaName, remoteConnection, infoConnection);
			infoConnection.close();
			remoteConnection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

class MultiMap<K, V> {
	// creating a map of key and value (collection)
	private Map<K, Collection<V>> map = new HashMap<>();

	// add the specified value with the specified key in this multimap
	public void put(K key, V value) {
		if (map.get(key) == null) {
			map.put(key, new ArrayList<V>());
		}
		map.get(key).add(value);
	}

	// associate the specified key with the given value if not already associated
	// with a value
	public void putIfAbsent(K key, V value) {
		if (map.get(key) == null) {
			map.put(key, new ArrayList<>());
		}
		// if the value is absent, insert it
		if (!map.get(key).contains(value)) {
			map.get(key).add(value);
		}
	}

	// the method returns the Collection of values to which the specified key is
	// mapped, or null if this multimap contains no mapping for the key
	public Collection<V> get(Object key) {
		return map.get(key);
	}

	// the method returns a set view of the keys contained in this multimap
	public Set<K> keySet() {
		return map.keySet();
	}

	// the method returns a set view of the mappings contained in this multimap
	public Set<Map.Entry<K, Collection<V>>> entrySet() {
		return map.entrySet();
	}

	// the method returns a Collection view of Collection of the values present in
	// this multimap
	public Collection<Collection<V>> values() {
		return map.values();
	}

	// Returns true if this multimap contains a mapping for the specified key.
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	// Removes the mapping for the specified key from this multimap if present and
	// returns the Collection of previous values associated with the key, or null if
	// there was no mapping for key
	public Collection<V> remove(Object key) {
		return map.remove(key);
	}

	// Returns the total number of key-value mappings in this multimap.
	public int size() {
		int size = 0;
		for (Collection<V> value : map.values()) {
			size += value.size();
		}
		return size;
	}

	// Returns true if this multimap contains no key-value mappings.
	public boolean isEmpty() {
		return map.isEmpty();
	}

	// Removes all the mappings from this multimap.
	public void clear() {
		map.clear();
	}

	// Removes the entry for the specified key only if it is currently mapped to the
	// specified value and returns true if removed
	public boolean remove(K key, V value) {
		if (map.get(key) != null) // key exists
			return map.get(key).remove(value);
		return false;
	}

	// Replaces the entry for the specified key only if currently mapped to the
	// specified value and return true if replaced
	public boolean replace(K key, V oldValue, V newValue) {
		if (map.get(key) != null) {
			if (map.get(key).remove(oldValue)) {
				return map.get(key).add(newValue);
			}
		}
		return false;
	}
}