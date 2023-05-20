import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {

    private int pageNumber;

    private String tableName;

    private final Vector<Hashtable<String, Object>> tuples;

    public Page(String tableName, int pageNumber) {
        tuples = new Vector<>();
        this.pageNumber = pageNumber;
        this.tableName = tableName;
    }

    public int getCurrentRowCount() {
        return tuples.size();
    }


    public Object getMinKey(String clusteringKey) {
        if (tuples.size() == 0) {
            return null;
        }
        return tuples.get(0).get(clusteringKey);
    }


    public Object getMaxKey(String clusteringKey) {
        return tuples.get(tuples.size()-1).get(clusteringKey);
    }

    public Vector<Hashtable<String, Object>> getTuples() {
        return tuples;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public String getTableName() {
        return tableName;
    }

    public void insertIntoPage(Hashtable<String, Object> htblColNameValue, String strClusteringKeyColumn) {
        int start = 0;
        int end = tuples.size() - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            Hashtable<String, Object> currentTuple = tuples.get(mid);
            Object currentClusteringKey = currentTuple.get(strClusteringKeyColumn);
            Object clusteringKeyValue = htblColNameValue.get(strClusteringKeyColumn);
            switch (currentClusteringKey.getClass().getSimpleName()) {
                case "String":
                    if (((String) currentClusteringKey).compareTo((String) clusteringKeyValue) > 0) {
                        end = mid - 1;
                    } else if (((String) currentClusteringKey).compareTo((String) clusteringKeyValue) < 0) {
                        start = mid + 1;
                    }
                    break;
                case "Integer":
                    if (((Integer) currentClusteringKey).compareTo((Integer) clusteringKeyValue) > 0) {
                        end = mid - 1;
                    } else if (((Integer) currentClusteringKey).compareTo((Integer) clusteringKeyValue) < 0) {
                        start = mid + 1;
                    }
                    break;
                case "Double":
                    if (((Double) currentClusteringKey).compareTo((Double) clusteringKeyValue) > 0) {
                        end = mid - 1;
                    } else if (((Double) currentClusteringKey).compareTo((Double) clusteringKeyValue) < 0) {
                        start = mid + 1;
                    }
                    break;
                case "Date":
                    if (((Date) currentClusteringKey).compareTo((Date) clusteringKeyValue) > 0) {
                        end = mid - 1;
                    } else if (((Date) currentClusteringKey).compareTo((Date) clusteringKeyValue) < 0) {
                        start = mid + 1;
                    }
                    break;
            }
        }
        tuples.add(start, htblColNameValue);
    }

    public int binarySearch(String clusteringKeyColumn, Object clusteringKeyValue) {
        int start = 0;
        int end = tuples.size() - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            Hashtable<String, Object> currentTuple = tuples.get(mid);
            Object clusteringKey = currentTuple.get(clusteringKeyColumn);
            // check type and correctly cast (cant find a better way to do it)
            switch (clusteringKey.getClass().getSimpleName()) {
                case "String":
                    if (((String) clusteringKey).compareTo((String) clusteringKeyValue) > 0) {
                        end = mid - 1;
                    } else if (((String) clusteringKey).compareTo((String) clusteringKeyValue) < 0) {
                        start = mid + 1;
                    } else {
                        return mid;
                    }
                    break;
                case "Integer":
                    if (((Integer) clusteringKey).compareTo((Integer) clusteringKeyValue) > 0) {
                        end = mid - 1;
                    } else if (((Integer) clusteringKey).compareTo((Integer) clusteringKeyValue) < 0) {
                        start = mid + 1;
                    } else {
                        return mid;
                    }
                    break;
                case "Double":
                    if (((Double) clusteringKey).compareTo((Double) clusteringKeyValue) > 0) {
                        end = mid - 1;
                    } else if (((Double) clusteringKey).compareTo((Double) clusteringKeyValue) < 0) {
                        start = mid + 1;
                    } else {
                        return mid;
                    }
                    break;
                case "Date":
                    if (((Date) clusteringKey).compareTo((Date) clusteringKeyValue) > 0) {
                        end = mid - 1;
                    } else if (((Date) clusteringKey).compareTo((Date) clusteringKeyValue) < 0) {
                        start = mid + 1;
                    } else {
                        return mid;
                    }
                    break;
            }
        }
        return -1; //not found
    }
}

