import java.io.*;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.*;

public class DBApp {
//    Vector<Table> tables= new Vector(10);

    public void init() {

    }

    public void createTable(String strTableName, String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax) throws DBAppException {
        String[] validTypes = {"java.lang.Integer", "java.lang.Double", "java.lang.String", "java.util.Date"};
        for (String key : htblColNameType.keySet()) {
            String type = htblColNameType.get(key);
            if (!Arrays.asList(validTypes).contains(type)) {
                throw new DBAppException("Invalid type");
            }
        }
        Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin,
                htblColNameMax);
        try {
            writeCsv(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
        } catch (IOException e) {
            throw new DBAppException("IO exception");
        }
        serializeTable(t);
        t = null;
    }

    private void writeCsv(String strTableName, String strClusteringKeyColumn,
                          Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
                          Hashtable<String, String> htblColNameMax) throws IOException {
        File f = new File("./src/resources/");
        f.mkdirs();
        FileWriter br = new FileWriter("./src/resources/metadata.csv", true);
        Enumeration<String> e = htblColNameType.keys();
        while (e.hasMoreElements()) {
            String key = e.nextElement();
            br.append(strTableName + ", ");
            br.append(key + ", ");
            br.append(htblColNameType.get(key) + ", ");
            if (key.equals(strClusteringKeyColumn))
                br.append("True, ");
            else
                br.append("False, ");
            br.append("null, ");
            br.append("null, ");

            br.append(String.valueOf(htblColNameMin.get(key)) + ", ");
            br.append(String.valueOf(htblColNameMax.get(key)) + ", ");
            br.append('\n');
        }
        br.flush();
        br.close();
    }


    public boolean checkTypes(String strTableName, Hashtable<String, Object> htblColNameValue) throws IOException, DBAppException {
        for (String key : htblColNameValue.keySet()) {
            Object value = htblColNameValue.get(key);
            BufferedReader reader = new BufferedReader(new FileReader("./src/resources/metadata.csv"));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(", ");
                if (strTableName.equals(values[0]) && key.equals(values[1])) {
                    if (value instanceof String) {
                        if (!values[2].equals("java.lang.String")) {
                            return false;
                        }
                    } else if (value instanceof Integer) {
                        if (!values[2].equals("java.lang.Integer")) {
                            return false;
                        }
                    } else if (value instanceof Double) {
                        if (!values[2].equals("java.lang.Double")) {
                            return false;
                        }
                    } else if (value instanceof Date) {
                        if (!values[2].equals("java.util.Date")) {
                            return false;
                        }
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                        formatter.setLenient(false);
                        try {
                            Date date = formatter.parse(value.toString());
                        } catch (ParseException e) {
                            throw new DBAppException("Invalid date format");
                        }
                    }
                }
            }
        }
        return true;
    }

    public boolean checkInRange(String strTableName, Hashtable<String, Object> htblColNameValue) throws IOException, ParseException {
        for (String key : htblColNameValue.keySet()) {
            Object value = htblColNameValue.get(key);
            BufferedReader reader = new BufferedReader(new FileReader("./src/resources/metadata.csv"));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(", ");
                if (strTableName.equals(values[0]) && key.equals(values[1])) {
                    String minString = values[6];
                    String maxString = values[7];
                    Object min = null;
                    Object max = null;
                    switch (values[2]) {
                        case "java.lang.String":
                            min = minString;
                            max = maxString;
                            break;
                        case "java.lang.Integer":
                            min = Integer.parseInt(minString);
                            max = Integer.parseInt(maxString);
                            break;
                        case "java.lang.Double":
                            min = Double.parseDouble(minString);
                            max = Double.parseDouble(maxString);
                            break;
                        case "java.lang.Date":
                            SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
                            min = s.parse(minString);
                            max = s.parse(maxString);
                            break;
                    }
                    if ((genericCompare(value, min) < 0 || genericCompare(value, max) > 0)) {
                        return false;
                    }
                }
            }
            reader.close();
        }
        return true;
    }

    //Check that index is requested for 3 colomns & that we're entering correct colomn names that exist in our table

    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {

        int count = 0;
        String indexName = "";
        Vector<NameAndType> nameAndType = new Vector<>();
        for (int i = 0; i < strarrColName.length; i++) {

            String colomnName = strarrColName[i];
            BufferedReader reader;
            try {
                reader = new BufferedReader(new FileReader("./src/resources/metadata.csv"));
            } catch (FileNotFoundException e) {
                throw new DBAppException("File not found");
            }
            String line;
            try {
                while ((line = reader.readLine()) != null) {

                    String[] values = line.split(", ");
                    if (strTableName.equals(values[0]) && colomnName.equals(values[1])) {

                        indexName += colomnName;
                        count++;

                        nameAndType.add(new NameAndType(colomnName, values[2]));
                    }
                }
            } catch (IOException e) {
                throw new DBAppException("");
            }
        }
        
        if (count != 3) {
            throw new DBAppException("Check colomn names are correct and that you're creating an index on 3 colomns");
        } else {
            updateCsv(strarrColName, strTableName, indexName);
            Table t = deserializeTable(strTableName);
            Octree index1 = new Octree(indexName, nameAndType, getMaxEntriesOctreeNode());
            //System.out.println(index1.getNameAndType());
            Vector minmax;
            try {
                minmax = getMinMaxFromCsv(strTableName, nameAndType);
            } catch (IOException e) {
                throw new DBAppException("IO exception");
            } catch (ParseException e) {
                throw new DBAppException("Parse Exception");
            }
           // System.out.println((Point) minmax.get(0));
            index1.setMinPoint((Point)minmax.get(0));
            index1.setMaxPoint((Point)minmax.get(1));
            index1.setMidPoint(index1.getMidValue(index1.getMinPoint(),index1.getMaxPoint()));
           // System.out.println(index1.getMinPoint());
           // System.out.println(index1.getMidPoint());
           // System.out.println(index1.getMaxPoint());
            for(int i=0;i<t.getPageNumbers().size();i++) {
            	Page currentPage = deserializePage(t.getStrTableName(), t.getPageNumbers().get(i));
            	for(int j=0;j<currentPage.getCurrentRowCount();j++) {
            		Hashtable<String, Object> htblColNameValue=currentPage.getTuples().get(j);
            		Point p = new Point();
                    Vector<Object> values = new Vector();
                    for (int z=0;z<index1.getNameAndType().size();z++) {
                        values.add(htblColNameValue.get(index1.getNameAndType().get(z).getColomnName()));
                    }
                    
                    p.setX(values.get(0));
                    p.setY(values.get(1));
                    p.setZ(values.get(2));
                    if(index1.Search(p)!=null){
                    	index1.updateOctreeDuplicates(p,"./src/resources/tables/" + strTableName + "/page" + currentPage.getPageNumber() + ".ser" , strTableName, htblColNameValue.get(t.getStrClusteringKeyColumn()));
                    	serializeIndex(index1, strTableName);
                    	break;}
                    else{index1.insert(p, "./src/resources/tables/" + strTableName + "/page" + currentPage.getPageNumber() + ".ser", strTableName, htblColNameValue.get(t.getStrClusteringKeyColumn()));
                    }
            	}
            }
            serializeIndex(index1, strTableName);
            t.getIndexNames().add(indexName);
            serializeTable(t);

        }

    }

    public void updateCsv(String[] columnNames, String tableName, String indexname) {
        List<String> cols = new ArrayList<>(Arrays.asList(columnNames));
        try {
            File file = new File("./src/resources/metadata.csv");
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String data = "";
            String line = "";
            String[] tempArr;
            while ((line = br.readLine()) != null) {
                tempArr = line.split(", ");
                // System.out.println(Arrays.toString(tempArr));
                if (tempArr[0].equals(tableName) && cols.contains(tempArr[1])) {
                    tempArr[4] = indexname;
                    tempArr[5] = "Octree";

                }
                data += String.join(", ", tempArr) + "\n";
            }
            br.close();
            FileOutputStream out;
            out = new FileOutputStream("./src/resources/metadata.csv");
            out.write(data.getBytes());
            out.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public Vector<Point> getMinMaxFromCsv(String strTableName, Vector<NameAndType> htblColNameType) throws IOException, ParseException {
        Vector<Point> minAndMax = new Vector();
        Vector<Object> minP = new Vector();
        Vector<Object> maxP = new Vector();
        for (int i=0;i<htblColNameType.size();i++) {
        BufferedReader reader = new BufferedReader(new FileReader("./src/resources/metadata.csv"));
        String line;
        while ((line = reader.readLine()) != null) {
                 String[] values = line.split(", ");
//                System.out.println(key);
//                System.out.println(values[1]);
                if (strTableName.equals(values[0]) && htblColNameType.get(i).getColomnName().equals((values[1]))){
                    String minString = values[6];
                    String maxString = values[7];
                    Object min = null;
                    Object max = null;
                    switch (values[2]) {
                        case "java.lang.String":
                            min = minString;
                            max = maxString;
                            break;
                        case "java.lang.Integer":
                            min = Integer.parseInt(minString);
                            max = Integer.parseInt(maxString);
                            break;
                        case "java.lang.Double":
                            min = Double.parseDouble(minString);
                            max = Double.parseDouble(maxString);
                            break;
                        case "java.lang.Date":
                            SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
                            min = s.parse(minString);
                            max = s.parse(maxString);
                            break;
                    }
                    minP.add(min);
                    maxP.add(max);

                }
            } reader.close();
        }
     //   System.out.print(minP.get(0));
        Point p1 = new Point(minP.get(0), minP.get(1), minP.get(2));
        Point p2 = new Point(maxP.get(0), maxP.get(1), maxP.get(2));
        minAndMax.add(p1);
        minAndMax.add(p2);


        return minAndMax;
    }

    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        int pageToInsertOctree = 0; //not sure men de
        Boolean invalidTypes;
        try {
            invalidTypes = !checkTypes(strTableName, htblColNameValue);
        } catch (IOException e) {
            throw new DBAppException("IO exception");
        }
        if (invalidTypes) {
            throw new DBAppException("Invalid types");
        }

        Boolean notInRange;
        try {
            notInRange = !checkInRange(strTableName, htblColNameValue);
        } catch (IOException e) {
            throw new DBAppException("IO exception");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        if (notInRange) {
            throw new DBAppException("Values out of range");
        }

        Table t = deserializeTable(strTableName);
        Vector<Hashtable<String, Object>> recordsOverflow = new Vector(); //Update their pageReference in index
        Vector<Integer> oldPage = new Vector();
        Vector<Integer> newPageNumber = new Vector();
        Object clusteringKey = htblColNameValue.get(t.getStrClusteringKeyColumn());
        int index = 0;
        if (htblColNameValue.get(t.getStrClusteringKeyColumn()) == null) {
            serializeTable(t);
            throw new DBAppException("Clustering key cannot be null");
        }
        if (t.getPageNumbers().size() == 0) {
            Page newPage = new Page(t.getStrTableName(), t.getLastPageInserted() + 1);
            t.setLastPageInserted(t.getLastPageInserted() + 1);
            t.getPageNumbers().add(t.getLastPageInserted());
            newPage.insertIntoPage(htblColNameValue, t.getStrClusteringKeyColumn());
            pageToInsertOctree = newPage.getPageNumber();
            serializePage(newPage);
            serializeTable(t);
            
            for (String indexName : t.getIndexNames()) {
                Octree o = deserializeIndex(indexName, strTableName);
               
                Point p = new Point();
                Vector<Object> values = new Vector();
                for (int i=0;i<o.getNameAndType().size();i++) {
                    values.add(htblColNameValue.get(o.getNameAndType().get(i).getColomnName()));
                }
                
                p.setX(values.get(0));
                p.setY(values.get(1));
                p.setZ(values.get(2));
              //  System.out.println(pageToInsertOctree);
                if(o.Search(p)!=null){
                	o.updateOctreeDuplicates(p,"./src/resources/tables/" + strTableName + "/page" + pageToInsertOctree + ".ser" , strTableName, clusteringKey);
                	serializeIndex(o, strTableName);
                	break;}
                else{o.insert(p, "./src/resources/tables/" + strTableName + "/page" + pageToInsertOctree + ".ser", strTableName, clusteringKey);
                serializeIndex(o, strTableName);}
            }
            return;
        }

        
        for (int i = 0; i < t.getPageNumbers().size(); i++) {
            Page currentPage = deserializePage(t.getStrTableName(), t.getPageNumbers().get(i));
            if (currentPage.binarySearch(t.getStrClusteringKeyColumn(), htblColNameValue.get(t.getStrClusteringKeyColumn())) != -1) {
                throw new DBAppException("Duplicate Clustering Key");
            }
            Object currentMin = currentPage.getMinKey(t.getStrClusteringKeyColumn());
            Object currentMax= currentPage.getMinKey(t.getStrClusteringKeyColumn());
            Page nextPage = (i + 1 == t.getPageNumbers().size()) ? null : deserializePage(t.getStrTableName(), t.getPageNumbers().get(i + 1));
            if (nextPage != null) { // we still have more pages
                Object nextMin = nextPage.getMinKey(t.getStrClusteringKeyColumn());
                if (genericCompare(currentMin, clusteringKey) < 0 && genericCompare(nextMin, clusteringKey) > 0
                        || genericCompare(currentMin, clusteringKey) > 0) {
                    currentPage.insertIntoPage(htblColNameValue, t.getStrClusteringKeyColumn());
                    pageToInsertOctree = currentPage.getPageNumber();
                    serializePage(currentPage);
                    index = i;
                    break;
                }
            } else { // we reached the last page
                if (genericCompare(currentMin, clusteringKey) < 0 || genericCompare(currentMax,clusteringKey)>0) {
                    currentPage.insertIntoPage(htblColNameValue, t.getStrClusteringKeyColumn());
                    pageToInsertOctree = currentPage.getPageNumber();
                    serializePage(currentPage);
                    index = i;
                    break;
                }
            }
        }

        for (String indexName : t.getIndexNames()) {
            Octree o = deserializeIndex(indexName, strTableName);
           
            Point p = new Point();
            Vector<Object> values = new Vector();
            for (int i=0;i<o.getNameAndType().size();i++) {
                values.add(htblColNameValue.get(o.getNameAndType().get(i).getColomnName()));
            }
           
            p.setX(values.get(0));
            p.setY(values.get(1));
            p.setZ(values.get(2));
            if(o.Search(p)!=null){
            	o.updateOctreeDuplicates(p,"./src/resources/tables/" + strTableName + "/page" + pageToInsertOctree + ".ser" , strTableName, clusteringKey);
            	serializeIndex(o, strTableName);
            	
            	break;}
            
            else{o.insert(p, "./src/resources/tables/" + strTableName + "/page" + pageToInsertOctree + ".ser", strTableName, clusteringKey);
            serializeIndex(o, strTableName);
           }
           
        }
       
        for (int j = index; j < t.getPageNumbers().size(); j++) {
            Page currentPage = deserializePage(t.getStrTableName(), t.getPageNumbers().get(j));
            Page nextPage = (j + 1 == t.getPageNumbers().size()) ? null : deserializePage(t.getStrTableName(), t.getPageNumbers().get(j + 1));
            if (nextPage != null) { // we still have more pages
                if (currentPage.getCurrentRowCount() > getPageRowCount()) {
                    Hashtable<String, Object> overflow = currentPage.getTuples().remove(currentPage.getCurrentRowCount() - 1);
                    nextPage.insertIntoPage(overflow, t.getStrClusteringKeyColumn());
                    recordsOverflow.add(overflow);
                    
                    for (String indexName : t.getIndexNames()) {
                        Octree o = deserializeIndex(indexName, strTableName);
                       
                        Point p = new Point();
                        Vector<Object> values = new Vector();
                        for (int i=0;i<o.getNameAndType().size();i++) {
                            values.add(overflow.get(o.getNameAndType().get(i).getColomnName()));
                        }
                       
                        p.setX(values.get(0));
                        p.setY(values.get(1));
                        p.setZ(values.get(2));
                        o.updatePoint(p,"./src/resources/tables/" + strTableName + "/page" + nextPage.getPageNumber() + ".ser" , overflow.get(t.getStrClusteringKeyColumn()));
                        serializeIndex(o, strTableName);
                    }
                    
                    oldPage.add(currentPage.getPageNumber());
                    newPageNumber.add(nextPage.getPageNumber());
                    serializePage(currentPage);
                    serializePage(nextPage);
                } else {
                    break;
                }
            } else { // handle overflow part in index with search and update reference in index
                if (currentPage.getCurrentRowCount() > getPageRowCount()) {
                    Hashtable<String, Object> overflow = currentPage.getTuples().remove(currentPage.getCurrentRowCount() - 1);
                    Page newPage = new Page(t.getStrTableName(), t.getLastPageInserted() + 1);
                    for (String indexName : t.getIndexNames()) {
                        Octree o = deserializeIndex(indexName, strTableName);
                       
                        Point p = new Point();
                        Vector<Object> values = new Vector();
                        for (int i=0;i<o.getNameAndType().size();i++) {
                            values.add(overflow.get(o.getNameAndType().get(i).getColomnName()));
                        }
                       
                        p.setX(values.get(0));
                        p.setY(values.get(1));
                        p.setZ(values.get(2));
                        o.updatePoint(p,"./src/resources/tables/" + strTableName + "/page" + newPage.getPageNumber() + ".ser" , overflow.get(t.getStrClusteringKeyColumn()));
                        serializeIndex(o, strTableName);
                    }
                    t.setLastPageInserted(t.getLastPageInserted() + 1);
                    t.getPageNumbers().add(t.getLastPageInserted());
                    newPage.insertIntoPage(overflow, t.getStrClusteringKeyColumn());
                    serializePage(currentPage);
                    serializePage(newPage);
                }
                break;
            }
        }
        
        
        
        
        
        


        Vector<String> indices = t.getIndexNames();
        serializeTable(t);


    }

    public static int genericCompare(Object a, Object b) {
        if (a instanceof Integer)
            return ((Integer) a).compareTo((Integer) b);
        else if (a instanceof Double)
            return ((Double) a).compareTo((Double) b);
        else if (a instanceof Date || b instanceof Date) {
            if (a instanceof Date && b instanceof Date)
                return ((Date) a).compareTo((Date) b);
            else {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                String formata = a instanceof Date ? formatter.format(a) : (String) a;
                String formatb = b instanceof Date ? formatter.format(b) : (String) b;
                return (formata).compareTo(formatb);
            }
        } else if (a instanceof String)
            return ((String) a).compareTo((String) b);
        else
            return 0;
    }

    // gets row count from config file
    public int getPageRowCount() {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("./src/resources/DBApp.config"));
            return Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }


    public void updateTable(String strTableName, String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException {

        Table table = deserializeTable(strTableName);

        if (table == null) {
            throw new DBAppException("Table does not exist");
        }
        if (table.getPageNumbers().size() == 0)
            throw new DBAppException("Table is empty");

        if (htblColNameValue.containsKey(table.getStrClusteringKeyColumn())) {
            throw new DBAppException("Cannot update clustering key");
        }

        Boolean invalidTypes;
        try {
            invalidTypes = !checkTypes(strTableName, htblColNameValue);
        } catch (IOException e) {
            throw new DBAppException("IO exception");
        }
        if (invalidTypes) {
            throw new DBAppException("Invalid types");
        }

        Boolean notInRange;
        try {
            notInRange = !checkInRange(strTableName, htblColNameValue);
        } catch (IOException e) {
            throw new DBAppException("IO exception");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        if (notInRange) {
            throw new DBAppException("Values out of range");
        }

        String clusteringKeyCol = table.getStrClusteringKeyColumn();
        Object clusteringKeyValue = null;
        Page first = deserializePage(table.getStrTableName(), table.getPageNumbers().get(0));
        switch (first.getTuples().get(0).get(table.getStrClusteringKeyColumn()).getClass().getSimpleName()) {
            case "String":
                clusteringKeyValue = strClusteringKeyValue;
                break;
            case "Integer":
                clusteringKeyValue = Integer.parseInt(strClusteringKeyValue);
                break;
            case "Double":
                clusteringKeyValue = Double.parseDouble(strClusteringKeyValue);
                break;
            case "Date":
                SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    clusteringKeyValue = s.parse(strClusteringKeyValue);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                break;
        }

        updateFromOctrees(strTableName,htblColNameValue,clusteringKeyValue);
        }
   

//    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
//        Table table = deserializeTable(strTableName);
//        
//        if (table == null) {
//            throw new DBAppException("Table does not exist");
//        }
//        Object clusteringKey=htblColNameValue.get(table.getStrClusteringKeyColumn());
//        if (clusteringKey != null) { // if given clustering key
//            String clusteringKeyCol = table.getStrClusteringKeyColumn();
//            boolean found = false;
//            for (int i = 0; i < table.getPageNumbers().size(); i++) {
//                Page p = deserializePage(table.getStrTableName(), table.getPageNumbers().get(i));
//                int index = p.binarySearch(clusteringKeyCol, htblColNameValue.get(clusteringKeyCol));
//                if (index != -1) {
//                    p.getTuples().remove(p.getTuples().get(index));
//                    serializePage(p);
//                    serializeTable(table);
//                    found = true;
//                    if (p.getCurrentRowCount() == 0) {
//                        File f = new File("./src/resources/tables/" + p.getTableName() + "/page" + p.getPageNumber() + ".ser");
//                        f.delete();
//                        table.getPageNumbers().removeElement(p.getPageNumber());
//                    } else {
//                        serializePage(p);
//                    }
//                    serializeTable(table);
//                    break;
//                }
//            }
//            if (!found) {
//                throw new DBAppException("Clustering key value does not exist in table " + strTableName);
//            }
//        } else  {
//       	deleteFromAllOctrees(strTableName,htblColNameValue);
//       	serializeTable(table);
//       	return;}
//        
//        	// no clustering key, have to search the whole page
//           
//    }
    
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException{
            Table table = deserializeTable(strTableName);
            if (table == null) {
                throw new DBAppException("Table does not exist");
            }
            if(table.getIndexNames().size()==0){
            if (htblColNameValue.get(table.getStrClusteringKeyColumn()) != null) { // if given clustering key
                String clusteringKeyCol = table.getStrClusteringKeyColumn();
                boolean found = false;
                for (int i = 0; i < table.getPageNumbers().size(); i++) {
                    Page p = deserializePage(table.getStrTableName(), table.getPageNumbers().get(i));
                    int index = p.binarySearch(clusteringKeyCol, htblColNameValue.get(clusteringKeyCol));
                    if (index != -1) {
                        p.getTuples().remove(p.getTuples().get(index));
                        serializePage(p);
                        serializeTable(table);
                        found = true;
                        if (p.getCurrentRowCount() == 0) {
                            File f = new File("./src/resources/tables/" + p.getTableName() + "/page" + p.getPageNumber() + ".ser");
                            f.delete();
                            table.getPageNumbers().removeElement(p.getPageNumber());
                        } else {
                            serializePage(p);
                        }
                        serializeTable(table);
                        break;
                    }
                }
                if (!found) {
                    throw new DBAppException("Clustering key value does not exist in table " + strTableName);
                }
            } else { // no clustering key, have to search the whole page
                for (int i = 0; i < table.getPageNumbers().size(); i++) {
                    Page p = deserializePage(table.getStrTableName(), table.getPageNumbers().get(i));
                    Vector<Hashtable> tuplesToRemove = new Vector<>();
                    for (Hashtable<String, Object> tuple : p.getTuples()) {
                        boolean matching = true;
                        for (String key : htblColNameValue.keySet()) {
                            if (!htblColNameValue.get(key).equals(tuple.get(key))) {
                                matching = false;
                            }
                        }
                        if (matching) {
                            tuplesToRemove.add(tuple);
                        }
                    }
                    p.getTuples().removeAll(tuplesToRemove);
                    if (p.getCurrentRowCount() == 0) {
                        File f = new File("./src/resources/tables/" + p.getTableName() + "/page" + p.getPageNumber() + ".ser");
                        f.delete();
                        table.getPageNumbers().removeElement(p.getPageNumber());
                    } else {
                        serializePage(p);
                    }}
                }}
            
            else{
                	deleteFromAllOctrees(strTableName,htblColNameValue,htblColNameValue.get(table.getStrClusteringKeyColumn()));
                }
            serializeTable(table);
            }
        
    
    
  
    //assume en de el case en kol el colomn names elel index ma3mool 3aleeha dakhla
    public void deleteFromAllOctrees(String tableName,Hashtable<String,Object> values,Object clusteringKey) throws DBAppException{
    	Table t= (Table)(deserializeTable(tableName));
    	Vector<String> allIndexColEntered= new Vector();
    	Vector<String> notAllIndexColEntered= new Vector();
    	Vector<Hashtable> tuplesToRemove = new Vector<>();
    	boolean weHaveAllValues=false;
    for(int i=0;i<t.getIndexNames().size();i++){
    	   int count=0;
    	
    	   Octree o=(Octree)(deserializeIndex(t.getIndexNames().get(i),tableName));
    	   for(String key: values.keySet()){
    		   for(int j=0;j<o.getNameAndType().size();j++){
    		        if(key.equals(o.getNameAndType().get(j).getColomnName()))
    			       count++;}}
    	   
    	   if(count==o.getNameAndType().size()){
    	    	  
                   allIndexColEntered.add(o.getIndexName())   ;
                 //  System.out.println("HIII");
                  
    	       }else{
    	    	   notAllIndexColEntered.add(o.getIndexName());
    	     // System.out.println("AAAAAA");
       }
    	   serializeIndex(o,tableName);}
       
       Vector<Hashtable<String,Object>> tuplesToBeDeleted= new Vector();
       for (int i=0;i<allIndexColEntered.size();i++) {
           Octree o = (Octree)(deserializeIndex(allIndexColEntered.get(i), tableName));
           Vector<Object> values1= new Vector();
           Point p1 = new Point();
           for (int j=0;j<o.getNameAndType().size();j++) {
        	   String key=o.getNameAndType().get(j).getColomnName();
        	   Object a=values.get(key);
               values1.add(a);
           }
           
           p1.setX(values1.get(0));
           p1.setY(values1.get(1));
           p1.setZ(values1.get(2));
           References deletedPoint;
           if(o.Search(p1)!=null){
        	   deletedPoint= new References(p1);
        	   for(int l=0;l<o.Search(p1).getPageref().size();l++){
        		   deletedPoint.getPageref().add(o.Search(p1).getPageref().get(l));
        		   deletedPoint.getKeyValue().add(o.Search(p1).getKeyValue().get(l));}
        	   o.deleteFromOctree(p1,clusteringKey);}
           else{
        	   deletedPoint=null;
           }
           
           if(deletedPoint!=null){
            for(int k=0;k<deletedPoint.getPageref().size();k++){
            	Page page= (Page)(deserializePage2(tableName,deletedPoint.getPageref().get(k)));
            	//System.out.println("AAAAA");
            	Object ClusteringKey= deletedPoint.getKeyValue().get(k);
            	for(int m=0;m<page.getTuples().size() ;m++) {
            		if(genericCompare(page.getTuples().get(m).get(t.getStrClusteringKeyColumn()),ClusteringKey)==0){
            			Hashtable<String, Object> deleteFromOtherTrees= new Hashtable();
            		//	System.out.println("HEREE");
            		    deleteFromOtherTrees.putAll(page.getTuples().get(m));
            			tuplesToBeDeleted.add(deleteFromOtherTrees);
            			page.getTuples().remove(m);}}
            	
            	serializePage(page);}
            serializeTable(t);
           }
           serializeIndex(o,tableName);
           weHaveAllValues=true;}
       
       
       if(notAllIndexColEntered.size()!=0 && weHaveAllValues){
    	  // System.out.print("makan ghalat");
    for(int i=0;i<notAllIndexColEntered.size();i++){
    	Octree o= (Octree)(deserializeIndex(notAllIndexColEntered.get(i), tableName));
    	Vector<Object>Values1=new Vector();
    	for (int j=0;j<tuplesToRemove.size();j++) {
    		for(int m=0;m< o.getNameAndType().size();m++){
            Values1.add(tuplesToRemove.get(j).get((o.getNameAndType().get(m).getColomnName())));
        }
    		Point p= new Point(Values1.get(0),Values1.get(1),Values1.get(2));
    		o.deleteFromOctree(p,clusteringKey);
    		}
    	serializeIndex(o,tableName);
    	
    }
    serializeTable(t);
    }
       if(notAllIndexColEntered.size()!=0 && !weHaveAllValues){
    	   //System.out.println("makan sa7");
    	   Vector<Hashtable<String,Object>> nameAndValue= new Vector();
    	    if (values.get(t.getStrClusteringKeyColumn()) != null) { // if given clustering key
                String clusteringKeyCol = t.getStrClusteringKeyColumn();
                boolean found = false;
                for (int i = 0; i < t.getPageNumbers().size(); i++) {
                    Page p = deserializePage(t.getStrTableName(), t.getPageNumbers().get(i));
                    int index = p.binarySearch(clusteringKeyCol, values.get(clusteringKeyCol));
                    if (index != -1) {
                    	nameAndValue.add(p.getTuples().get(index));
                    	
                        p.getTuples().remove(p.getTuples().get(index));
                        serializePage(p);
                        serializeTable(t);
                        found = true;
                        if (p.getCurrentRowCount() == 0) {
                            File f = new File("./src/resources/tables/" + p.getTableName() + "/page" + p.getPageNumber() + ".ser");
                            f.delete();
                            t.getPageNumbers().removeElement(p.getPageNumber());
                        } else {
                            serializePage(p);
                        }
                        serializeTable(t);
                        break;
                    }
                }
                if (!found) {
                    throw new DBAppException("Clustering key value does not exist in table " + tableName);
                }
            } else {
            	//System.out.print("MAKAN SA7 2"); // no clustering key, have to search the whole page
                for (int i = 0; i < t.getPageNumbers().size(); i++) {
                    Page p = deserializePage(t.getStrTableName(), t.getPageNumbers().get(i));
                    for (Hashtable<String, Object> tuple : p.getTuples()) {
                        boolean matching = true;
                        for (String key : values.keySet()) {
                            if (!values.get(key).equals(tuple.get(key))) {
                                matching = false;
                            }
                        }
                        if (matching) {
                        	nameAndValue.add(tuple);
                        //	System.out.println(nameAndValue);
                            tuplesToRemove.add(tuple);
                        }
                    }
                    p.getTuples().removeAll(tuplesToRemove);
                    if (p.getCurrentRowCount() == 0) {
                        File f = new File("./src/resources/tables/" + p.getTableName() + "/page" + p.getPageNumber() + ".ser");
                        f.delete();
                        t.getPageNumbers().removeElement(p.getPageNumber());
                    } else {
                        serializePage(p);
                        serializeTable(t);
                    }}
                }
    	    for(int i=0;i<notAllIndexColEntered.size();i++){
    	    //	System.out.println("HENA");
    	    	 Octree o = (Octree)(deserializeIndex(notAllIndexColEntered.get(i), tableName));
    	           Vector<Object> values1= new Vector();
    	           Point p1 = new Point();
    	         //  System.out.println(o.getNameAndType().size());
    	          for(int j=0;j<nameAndValue.size();j++){
    	        	//  System.out.println(nameAndValue.size());
    	        	  for(int m=0;m<o.getNameAndType().size();m++){
    	        
    	        		 String key= o.getNameAndType().get(m).getColomnName();
    	        		// System.out.println(nameAndValue.get(j).get(key)+ key);
    	        		  values1.add(nameAndValue.get(j).get(key));
    	        	  }
    	    	   
    	           p1.setX(values1.get(0));
    	           p1.setY(values1.get(1));
    	           p1.setZ(values1.get(2));
    	           References deletedPoint;
    	           if(o.Search(p1)!=null){
    	        	//   System.out.println("O.search(p1)");
    	        	   deletedPoint= new References(p1);
    	        	   for(int l=0;l<o.Search(p1).getPageref().size();l++){
    	        		   deletedPoint.getPageref().add(o.Search(p1).getPageref().get(l));
    	        		   deletedPoint.getKeyValue().add(o.Search(p1).getKeyValue().get(l));}
    	        	   o.deleteFromOctree(p1,clusteringKey);}
    	           
    	           }
    	          serializeIndex(o,tableName);
    	    }
    	    	 
    	    }}
    
    public void updateFromOctrees(String tableName,Hashtable<String,Object> values,Object clusteringKey) throws DBAppException{
    	Table t= (Table)(deserializeTable(tableName));
    	Vector<Hashtable<String,Object>> oldValues = new Vector<>();
        Vector<Hashtable<String,Object>> newValues= new Vector();
        if (clusteringKey != null) { 
                String clusteringKeyCol = t.getStrClusteringKeyColumn();
                boolean found = false;
                for (int i = 0; i < t.getPageNumbers().size(); i++) {
                    Page p = deserializePage(t.getStrTableName(), t.getPageNumbers().get(i));
                    int index = p.binarySearch(clusteringKeyCol, clusteringKey);
                    if (index != -1) {
                    	Hashtable<String,Object> oldV= new Hashtable();
                    	for(String key: p.getTuples().get(index).keySet()){
                    	oldV.put(key,p.getTuples().get(index).get(key) );}
                    	
                    	oldValues.add(oldV);
                    	
                    	for (String key : values.keySet()) {
                            p.getTuples().get(index).put(key, values.get(key));
                        }
                    	newValues.add(p.getTuples().get(index));
                   // 	System.out.println(newValues.get(0));
                        serializePage(p);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new DBAppException("Clustering key value does not exist in table " + tableName);
                } 
    	    for(int i=0;i<t.getIndexNames().size();i++){
    	    	 Octree o = (Octree)(deserializeIndex(t.getIndexNames().get(i), tableName));
    	           Vector<Object> deleteThis= new Vector();
    	           Vector<Object> insertThis= new Vector();
    	           Point p1 = new Point();
    	           Point p2= new Point();
//    	           System.out.println(o.getNameAndType().size());
    	          for(int j=0;j<oldValues.size();j++){
    	        	  for(int m=0;m<o.getNameAndType().size();m++){       
    	        		 String key= o.getNameAndType().get(m).getColomnName();
    	        		// System.out.println("hello" + oldValues.get(j).get(key));
    	        		  deleteThis.add(oldValues.get(j).get(key));
    	        		  //System.out.println(key);
    	        		  insertThis.add(newValues.get(j).get(key));
    	        		 // System.out.println(insertThis.get(j));
    	        		 // System.out.println( "hi"+ " "+ newValues.get(j).get(key));
    	        	  }
    	           p1.setX(deleteThis.get(0));
    	           p1.setY(deleteThis.get(1));
    	           p1.setZ(deleteThis.get(2));
    	         //  System.out.println(p1.getX()+ " "+ p1.getY()+ " "+ p1.getZ());
    	         //  System.out.println(p2.getX()+ " "+ p2.getY()+ " "+ p2.getZ());
    	           p2.setX(insertThis.get(0));
    	           p2.setY(insertThis.get(1));
    	           p2.setZ(insertThis.get(2));
    	           o.updateoctree(p1, p2, clusteringKey, tableName);
    	           serializeIndex(o,tableName);
    	           }
    	          serializeIndex(o,tableName);
    	    }
    	    serializeTable(t);
    	    }}
    	   
    
    //DE GEDEEDAA MAKE SURE IT WORKS
    public Page deserializePage2(String tableName,String directory){
    	try {
            FileInputStream fileIn = new FileInputStream(directory);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Page p = (Page) in.readObject();
            in.close();
            fileIn.close();
            return p;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Page not found");
            c.printStackTrace();
            return null;
        }
    }
    
    
    public void serializeTable(Table t) {
        try {
            File f = new File("./src/resources/tables/" + t.getStrTableName());
            f.mkdirs();
            FileOutputStream fileOut =
                    new FileOutputStream("./src/resources/tables/" + t.getStrTableName() + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(t);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public void serializeIndex(Octree o, String tableName) {
        try {
            File f = new File("./src/resources/tables/" + tableName);
            f.mkdirs();
            FileOutputStream fileOut =
                    new FileOutputStream("./src/resources/tables/" + tableName + "/" + o.getIndexName() + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(o);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public Octree deserializeIndex(String indexName, String tableName) {
        try {
            FileInputStream fileIn = new FileInputStream("./src/resources/tables/" + tableName + "/" + indexName + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Octree o = (Octree) in.readObject();
            in.close();
            fileIn.close();
            return o;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Table not found");
            c.printStackTrace();
            return null;
        }

    }


    public Table deserializeTable(String tableName) {
        try {
            FileInputStream fileIn = new FileInputStream("./src/resources/tables/" + tableName + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Table t = (Table) in.readObject();
            in.close();
            fileIn.close();
            return t;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Table not found");
            c.printStackTrace();
            return null;
        }
    }

    public void serializePage(Page p) {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream("./src/resources/tables/" + p.getTableName() + "/page" + p.getPageNumber() + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public Page deserializePage(String tableName, int pageNumber) {
        try {
            FileInputStream fileIn = new FileInputStream("./src/resources/tables/" + tableName + "/page" + pageNumber + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Page p = (Page) in.readObject();
            in.close();
            fileIn.close();
            return p;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Page not found");
            c.printStackTrace();
            return null;
        }
    }

    public void printTable(String tableName) {
        Table t = deserializeTable(tableName);
        System.out.println(t.getStrTableName());
        System.out.println("----------------------------------");
        for (int i = 0; i < t.getPageNumbers().size(); i++) {
            Page p = deserializePage(tableName, t.getPageNumbers().get(i));
            System.out.println("Page " + p.getPageNumber() + ": ");
            for (int j = 0; j < p.getTuples().size(); j++) {
                Hashtable<String, Object> tuple = p.getTuples().get(j);
                for (String key : tuple.keySet()) {
                    Object value = tuple.get(key);
                    System.out.print(key + ": " + value + ", ");
                }
                System.out.print("\n");
            }
            System.out.println("----------------------------------");

        }
    }

    public Vector<String> getOctrees(String strTableName) throws DBAppException {
        Vector<String> octrees = new Vector();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("./src/resources/metadata.csv"));
        } catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            throw new DBAppException("FileNotFoundException");

        }
        String line;

        try {
            while ((line = reader.readLine()) != null) {

                String[] values = line.split(", ");

                if (strTableName.equals(values[0]) && values[5].equals("Octree")) {
                    octrees.add(values[4]);
                }
            }
        } catch (IOException e) {
            throw new DBAppException("Invalid entry indexname");
        }
        for (int i = 0; i < octrees.size(); i++) {
            String indexName = octrees.get(i);
            int count = 0;
            for (int j = 0; j < octrees.size(); j++) {
                if (octrees.get(j).equals(indexName))
                    count++;
            }
            if (count > 1)
                octrees.remove(i);
        }
        return octrees;


    }

    public int getMaxEntriesOctreeNode() {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("./src/resources/DBApp.config"));
            return Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }
    
    public  Vector<String> IndicesCreated(SQLTerm [] terms, String[] strarrOperators){
    	Table t= (Table) deserializeTable(terms[0]._strTableName);
    	Vector<String> indexName= new Vector();
    	ArrayList<ArrayList> positions= new ArrayList();
    	for(int k=0;k<t.getIndexNames().size();k++){
    	Octree o= deserializeIndex(t.getIndexNames().get(k), terms[0]._strTableName);
    	int Count=0;
    	for(int i=0;i<terms.length;i++){
    		
    	            for(int j=i+1;j< terms.length;j++){
    	         
    	                      for(int m=j+1;m<terms.length;m++){
    	                    	 
    	                    	  String col1= o.getNameAndType().get(0).getColomnName();
    	                    	  String col2= o.getNameAndType().get(1).getColomnName();
    	                    	  String col3= o.getNameAndType().get(2).getColomnName();
    	                    	  
    	                      if((terms[i]._strColumnName.equals(col1) && terms[j]._strColumnName.equals(col2) && terms[m]._strColumnName.equals(col3))|| 
    	                    		  (terms[i]._strColumnName.equals(col1) && terms[j]._strColumnName.equals(col3) && terms[m]._strColumnName.equals(col2)) ||
    	                    		  (terms[i]._strColumnName.equals(col2) && terms[j]._strColumnName.equals(col1) && terms[m]._strColumnName.equals(col3)) ||
    	                    		  (terms[i]._strColumnName.equals(col2) && terms[j]._strColumnName.equals(col3) && terms[m]._strColumnName.equals(col1)) ||
    	                    		  (terms[i]._strColumnName.equals(col3) && terms[j]._strColumnName.equals(col1) && terms[m]._strColumnName.equals(col2)) ||
    	                    		  (terms[i]._strColumnName.equals(col3) && terms[j]._strColumnName.equals(col2) && terms[m]._strColumnName.equals(col1))){
    	                    	  indexName.add(t.getIndexNames().get(k));
    	                    	  ArrayList <Integer> a= new ArrayList();
    	                    	  a.add(i);
    	                    	  a.add(j);
    	                    	  a.add(m);
    	                    	  positions.add(a);
    	                      }
    	}
    	}}}
    	Vector returnIndices= new Vector();
    	for(int i=0;i<checkForIndexDuplicates(indexName,positions,strarrOperators).size();i++){
    		returnIndices.add(checkForIndexDuplicates(indexName,positions,strarrOperators).get(i));
    	}
    	serializeTable(t);
    	return returnIndices;}

    public Vector<Hashtable<String,Object>> SelectUsingIndex(SQLTerm[]arrSQLTerms,String []strarrOperators, Vector<String> indices){
    	Table table= (Table)deserializeTable(arrSQLTerms[0]._strTableName);
    	Vector<Hashtable<String,Object>> resultSet= new Vector();
    	for(int i=0;i<indices.size();i++){
    		Octree o=(Octree)deserializeIndex(indices.get(i),table.getStrTableName());
    		Vector<String> colNames= new Vector();
    		colNames.add(o.getNameAndType().get(0).getColomnName());
    		colNames.add(o.getNameAndType().get(1).getColomnName());
    		colNames.add(o.getNameAndType().get(2).getColomnName());
    		Point p= new Point();
    		Vector<Object> values= new Vector();
    		Vector <String> equalitySign= new Vector();
    		for(int j=0;j<colNames.size();j++){
    			for(int k=0;k<arrSQLTerms.length;k++){
    			if(colNames.get(j).equals(arrSQLTerms[k]._strColumnName)){
    				values.add(arrSQLTerms[k]._objValue);
    				equalitySign.add(arrSQLTerms[k]._strOperator);
    			}}}
    		p.setX(values.get(0));
    		p.setY(values.get(1));
    		p.setZ(values.get(2));
    		
    		References r;
    		r= new References(p);
    		if((o.Search(p)!=null)){
    		if(equalitySign.get(0).equals("=") && equalitySign.get(1).equals("=") && equalitySign.get(2).equals("=")){
    			for(int n=0;n<o.Search(p).getPageref().size();n++){
    				r.getPageref().add(o.Search(p).getPageref().get(n));
    			}
    			for(int l=0;l<r.getPageref().size();l++){
    				Page page=(Page)deserializePage2(table.getStrTableName(),r.getPageref().get(l));
    				for(int h=0;h<page.getTuples().size();h++ ){
    					
    					if(genericCompare(page.getTuples().get(h).get(colNames.get(0)),p.getX())==0 &&
    							genericCompare(page.getTuples().get(h).get(colNames.get(1)),p.getY())==0
    							&& genericCompare(page.getTuples().get(h).get(colNames.get(2)),p.getZ())==0){
    						resultSet.add(page.getTuples().get(h));
    					}
    				}
    				
    				
    			}
    			
    			
    		}}
    		
    	
    	
    	
    } return resultSet;}
    
    
    
    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        String[] operators = {">", ">=", "<", "<=", "!=", "="};
        for (SQLTerm term : arrSQLTerms) {
            if (!Arrays.asList(operators).contains(term._strOperator)) {
                throw new DBAppException("Invalid operator");
            }
        }
        for (String operator : strarrOperators) {
            if (!(operator.equals("AND") || operator.equals("OR") || operator.equals("XOR"))) {
                throw new DBAppException("Invalid operator");
            }
        }
        if (arrSQLTerms.length != strarrOperators.length + 1) {
            throw new DBAppException("Not Enough Operators");
        }
        Table table = deserializeTable(arrSQLTerms[0]._strTableName);
        
        if (table == null) {
            throw new DBAppException("Table does not exist");
        }
        Vector<Vector<Hashtable<String, Object>>> setsToIntersect = new Vector<>();
        for (SQLTerm term : arrSQLTerms) {

            setsToIntersect.add(selectTuples(term, table));
        }
       
        	
        
        if (arrSQLTerms.length == 1) {
            return setsToIntersect.get(0).iterator();
        }
        int index1=0;
        int index = 0;
        System.out.println(setsToIntersect.size());
        Vector<SQLTerm> x=new Vector();
    	SQLTerm[] z=new SQLTerm[3];
    	for(int i=0;i<arrSQLTerms.length;i++) {
    		if((arrSQLTerms[i]!=null))
    		x.add(arrSQLTerms[i]);
    	}
    	Boolean Flag=true;
        while (setsToIntersect.size() > 1) {
        	
        	for(int i=0;i<z.length;i++) {
        		if(x.size()>i)
        		z[i]=x.get(i);
        	}
        	
        	if(setsToIntersect.size()>= 3) {
        	if(x.size()>=3&&Flag) {
        	if(IndicesCreated(z,Arrays.copyOfRange(strarrOperators, index, index+2)).size()!=0) {
        		setsToIntersect.remove(0);
        		setsToIntersect.remove(0);
        		setsToIntersect.remove(0);
        		x.remove(0);
        		x.remove(0);
        		x.remove(0);
             	setsToIntersect.add(0,SelectUsingIndex(z,Arrays.copyOfRange(strarrOperators, index, index+2),IndicesCreated(Arrays.copyOfRange(arrSQLTerms, index1, index1+3),Arrays.copyOfRange(strarrOperators, index, index+2))));
             	System.out.println(setsToIntersect.size());
             	x.add(0,new SQLTerm());
             	
             	index+=2;
             	index1+=3;
             	continue;
        	}}}
        	Vector<Hashtable<String, Object>> set1 = setsToIntersect.remove(0);
            Vector<Hashtable<String, Object>> set2 = setsToIntersect.remove(0);
            x.remove(0);
            x.remove(0);
            Flag=false;
            Vector<Hashtable<String, Object>> set3 = joinResults(set1, set2, strarrOperators[index]);
            System.out.println(set3+"Set3");
            setsToIntersect.add(0, set3);
            x.add(0,new SQLTerm());
            
            
        	index1=index1+2;
            index++;
           
        }
        return setsToIntersect.get(0).iterator();
    }

    public Vector<Hashtable<String, Object>> selectTuples(SQLTerm term, Table table) {
        Vector<Hashtable<String, Object>> resultSet = new Vector();
        for (int i : table.getPageNumbers()) {
            Page p = deserializePage(table.getStrTableName(), i);
            for (Hashtable tuple : p.getTuples()) {
                Object value = tuple.get(term._strColumnName);
                switch (term._strOperator) {
                    case "=":
                        if (genericCompare(value, term._objValue) == 0) {
                            resultSet.add(tuple);
                        }
                        break;
                    case ">":
                        if (genericCompare(value, term._objValue) > 0) {
                            resultSet.add(tuple);
                        }
                        break;
                    case "<":
                        if (genericCompare(value, term._objValue) < 0) {
                            resultSet.add(tuple);
                        }
                        break;
                    case ">=":
                        if (genericCompare(value, term._objValue) >= 0) {
                            resultSet.add(tuple);
                        }
                        break;
                    case "<=":
                        if (genericCompare(value, term._objValue) <= 0) {
                            resultSet.add(tuple);
                        }
                        break;
                    case "!=":
                        if (genericCompare(value, term._objValue) != 0) {
                            resultSet.add(tuple);
                        }
                        break;
                }
            }
        }
        return resultSet;
    }

    public Vector<Hashtable<String, Object>> joinResults(Vector<Hashtable<String, Object>> set1,
            Vector<Hashtable<String, Object>> set2, String operator) {
     Vector<Hashtable<String, Object>> resultSet = new Vector();
             Vector<Hashtable<String, Object>> totalll = new Vector();
             switch (operator) {
case "AND":
for (Hashtable tuple : set1) {
if (set2.contains(tuple)) {
resultSet.add(tuple);
}
}
break;
case "OR":

for (Hashtable tuple : set1) {

totalll.add(tuple);

}
for (Hashtable tuple : set2) {
if (!totalll.contains(tuple)) {
totalll.add(tuple);
}
}

for (Hashtable tuple : totalll) {
resultSet.add(tuple);
}

break;
case "XOR":
for (Hashtable tuple : set1) {
if (!set2.contains(tuple))
resultSet.add(tuple);
}
for (Hashtable tuple : set2) {
if (!set1.contains(tuple))
resultSet.add(tuple);
}
break;
}

return resultSet;
    }
    public Vector<String> checkForIndexDuplicates(Vector<String> duplicatedIndices, ArrayList<ArrayList> positions, String strarrOperators[]){
    	for(int i=0;i<duplicatedIndices.size();i++){
    		for(int j=i+1;j<duplicatedIndices.size();j++){
    			if(duplicatedIndices.get(i).equals(duplicatedIndices.get(j))){
    				duplicatedIndices.remove(j);
    				positions.remove(j);
    		}}
    	}
    	
    	for(int i=0;i<duplicatedIndices.size();i++){
    		int pos1= (int)positions.get(i).get(0);
    		int pos2= (int)positions.get(i).get(1);
    		int pos3= (int)positions.get(i).get(2);
    		if(!((strarrOperators[pos2-1].equals("AND")) && strarrOperators[pos3-1].equals("AND")))
    			duplicatedIndices.remove(i);
    	}
    	
    	return duplicatedIndices;
    }


    public static void main(String[] args) {
    	
//     
       DBApp dbApp = new DBApp();

       String strTableName = "Student";
       SQLTerm [] term= new SQLTerm[4];
       term[0]= new SQLTerm();
       term[0]._strColumnName="gpa";
       term[0]._strTableName="Student";
       term[0]._strOperator="=";
       term[0]._objValue=3.0;
       term[1]= new SQLTerm();
       term[1]._strColumnName="age";
       term[1]._strTableName="Student";
       term[1]._strOperator="=";
       term[1]._objValue=15;
       term[2]= new SQLTerm();
       term[2]._strColumnName="name";
       term[2]._strTableName="Student";
       term[2]._strOperator="=";
       term[2]._objValue="zeyad";
       term[3]= new SQLTerm();
       term[3]._objValue=23;
       term[3]._strColumnName="id";
       term[3]._strOperator="=";
       term[3]._strTableName="Student";
       String []strarrOperator= new String [3];
       strarrOperator[0]="AND";
       strarrOperator[1]="AND";
       strarrOperator[2]="OR";
       try {
		dbApp.selectFromTable(term, strarrOperator);
	} catch (DBAppException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
//        Hashtable htblColNameType = new Hashtable();
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("gpa", "java.lang.Double");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("age", "java.lang.Integer");
//
//        Hashtable<String, String> min = new Hashtable();
//        Hashtable<String, String> max = new Hashtable();
//        min.put("id", "1");
//        min.put("gpa", "0.7");
//        min.put("name", "a");
//        min.put("age", "1");
//        max.put("id", "10000");
//        max.put("gpa", "4");
//        max.put("name", "zzzzzzzz");
//        max.put("age", "30");
//
//        try {
//            dbApp.createTable(strTableName, "id", htblColNameType, min, max);
//        } catch (DBAppException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//
//        String[] a = new String[3];
//        a[0] = "gpa";
//        a[1] = "name";
//        a[2] = "age";
//        try {
//            dbApp.createIndex(strTableName, a);
//        } catch (DBAppException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//
//        Hashtable htblColNameValue3 = new Hashtable();
//        htblColNameValue3.put("id",22);
//        htblColNameValue3.put("gpa", 3.0);
//        htblColNameValue3.put("name", "zeyad");
//        htblColNameValue3.put("age", 15);
//        try {
//            dbApp.insertIntoTable(strTableName, htblColNameValue3);
//        } catch (DBAppException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("id", 23);
//        htblColNameValue.put("gpa", 2.5);
//        htblColNameValue.put("name", "wael");
//        htblColNameValue.put("age", 20);
//        try {
//            dbApp.insertIntoTable(strTableName, htblColNameValue);
//        } catch (DBAppException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        Hashtable htblColNameValue1 = new Hashtable();
//        htblColNameValue1.put("id", 6);
//        htblColNameValue1.put("gpa", 3.0);
//        htblColNameValue1.put("name", "abouz");
//        htblColNameValue1.put("age", 15);
//        try {
//            dbApp.insertIntoTable(strTableName, htblColNameValue1);
//        } catch (DBAppException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        Hashtable htblColNameValue2 = new Hashtable();
//        htblColNameValue2.put("id", 20);
//        htblColNameValue2.put("gpa", 2.0);
//        htblColNameValue2.put("name", "mariooma");
//        htblColNameValue2.put("age", 9);
//        try {
//            dbApp.insertIntoTable(strTableName, htblColNameValue2);
//        } catch (DBAppException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        Hashtable htblColNameValue5 = new Hashtable();
//      htblColNameValue5.put("id", 13);
//      htblColNameValue5.put("gpa", 2.0);
//      htblColNameValue5.put("name", "marwan");
//      htblColNameValue5.put("age", 10);
//      try {
//          dbApp.insertIntoTable(strTableName, htblColNameValue5);
//      } catch (DBAppException e) {
//          // TODO Auto-generated catch block
//          e.printStackTrace();
//      }
//////        
//        Hashtable <String,Object> values= new Hashtable();
//        values.put("age",22);
//        values.put("gpa", 3.0);
//       
//        
//       
//        
//        try {
//			dbApp.updateTable(strTableName,"22", values);
//		} catch (DBAppException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
       Table t = dbApp.deserializeTable(strTableName);
        dbApp.printTable(strTableName);
        Octree o = dbApp.deserializeIndex(t.getIndexNames().get(0), strTableName);
        dbApp.serializeIndex(o,strTableName);
        o.printOctree();
//      
//       
//   


//        Hashtable htblColNameType = new Hashtable();
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("gpa", "java.lang.Double");
//        htblColNameType.put("age", "java.lang.Integer");
//
//        Octree o = new Octree("index", htblColNameType, new Point(1,1,1), new Point(10,10,10), dbApp.getMaxEntriesOctreeNode());
//        String r= "./src/resources/tables/" + "Student" + "/page" + "1" + ".ser";
//        o.insert(new Point(1, 2, 3),"./src/resources/tables/" + "Student" + "/page" + "1" + ".ser","Student", 432);
//        o.insert(new Point(4, 7, 3),"./src/resources/tables/" + "Student" + "/page" + "2" + ".ser","Student", 13124);
//        o.insert(new Point(7, 3, 2),"./src/resources/tables/" + "Student" + "/page" + "3" + ".ser","Student", 2535424);
//        o.insert(new Point(10, 2, 8),"./src/resources/tables/" + "Student" + "/page" + "4" + ".ser","Student", 204492);
//        o.insert(new Point(5, 7, 1),"./src/resources/tables/" + "Student" + "/page" + "5" + ".ser","Student", 232);
//        o.insert(new Point(1,1,1),"./src/resources/tables/" + "Student" + "/page" + "6" + ".ser","Student", 424);
//        o.insert(new Point(2, 2, 2),"./src/resources/tables/" + "Student" + "/page" + "7" + ".ser","Student", 32);
//        o.insert(new Point(3, 3, 3),"./src/resources/tables/" + "Student" + "/page" + "8" + ".ser","Student", 23);
//        System.out.println(o.getNameAndType());
//        dbApp.serializeIndex(o, strTableName);
//        Octree c = dbApp.deserializeIndex("index", strTableName);
//        System.out.println(c.getNameAndType());
//
//        o.printOctree();


    }
    }
    



