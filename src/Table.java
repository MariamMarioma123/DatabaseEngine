import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private final String strTableName;
    private final String strClusteringKeyColumn;
    private Hashtable<String, String> htblColNameType;
    private Hashtable<String, String> htblColNameMin;
    private Hashtable<String, String> htblColNameMax;
    private Vector<String> indexNames;
    private int lastPageInserted;

    private Vector<Integer> pageNumbers;

    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
                 Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) {
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        htblColNameType = new Hashtable<>();
        htblColNameMin = new Hashtable<>();
        htblColNameMax = new Hashtable<>();
        lastPageInserted = 0;
        pageNumbers = new Vector<>();
        indexNames= new Vector();
    }

    public Vector<String> getIndexNames() {
		return indexNames;
	}



	public String getStrTableName() {
        return strTableName;
    }

    public String getStrClusteringKeyColumn() {
        return strClusteringKeyColumn;
    }

    public Hashtable<String, String> getHtblColNameType() {
        return htblColNameType;
    }

    public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
        this.htblColNameType = htblColNameType;
    }

    public Hashtable<String, String> getHtblColNameMin() {
        return htblColNameMin;
    }

    public void setHtblColNameMin(Hashtable<String, String> htblColNameMin) {
        this.htblColNameMin = htblColNameMin;
    }

    public Hashtable<String, String> getHtblColNameMax() {
        return htblColNameMax;
    }

    public void setHtblColNameMax(Hashtable<String, String> htblColNameMax) {
        this.htblColNameMax = htblColNameMax;
    }

    public Vector<Integer> getPageNumbers() {
        return pageNumbers;
    }

    public int getLastPageInserted() {
        return lastPageInserted;
    }

    public void setLastPageInserted(int n) {
        lastPageInserted = n;
    }

//    public void insertIntoTable(int pageNumber, Hashtable<String, Object> htblColNameValue) {
//        if (pages.get(pageNumber).getCurrentRowCount() < pages.get(pageNumber).getMaximumRowCount()) {
//            pages.get(pageNumber).insertIntoPage(htblColNameValue, String.valueOf(strClusteringKeyColumn));
//        } else {
//            pages.get(pageNumber).insertIntoPage(htblColNameValue, String.valueOf(strClusteringKeyColumn));
//            for (int i = pageNumber; i < pages.size() - 1; i++) {
//                int tupleShiftedIndex = pages.get(pageNumber).getMaximumRowCount();
//                pages.get(pageNumber + 1).getTuples().add(0, pages.get(pageNumber).getTuples()
//                        .get(tupleShiftedIndex));
//
//            }
//        }
//        for (int i = 0; i < pages.size(); i++) {
//            pages.get(i).setCurrentRowCount(pages.get(i).getTuples().size());
//        }
//    }

    public Vector<Hashtable<String, Object>> selectTuples(String columnName, String operator, Object value) {
        Vector<Hashtable<String, Object>> results = new Vector<>();

        switch (operator) {
            case ">":
                break;
            case ">=":
                break;
            case "<":
                break;
            case "<=":
                break;
            case "!=":
                break;
            case "=":
                break;
        }
        return results;
    }
}
