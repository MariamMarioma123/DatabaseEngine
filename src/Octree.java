import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;


public class Octree implements Serializable {

    private String indexName;
    private Vector<NameAndType> nameAndType;
    private Vector<Octree> children;
//    private Hashtable<String,Object> minHash;
//    private Hashtable<String, Object> maxHash;
    private Vector<References> points;
    private int maxNumberOfPoints;
    private Point maxPoint;
    private Point minPoint;
    private Point midPoint;

    
    public Vector<String> getColumnNames() {
        Vector<String> result = new Vector<>();
        for (NameAndType n : nameAndType) {
            result.add(n.getColomnName());
        }
        return result;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Point getMaxPoint() {
        return maxPoint;
    }

    public void setMaxPoint(Point maxPoint) {
        this.maxPoint = maxPoint;
    }

    public Point getMinPoint() {
        return minPoint;
    }

    public void setMinPoint(Point minPoint) {
        this.minPoint = minPoint;
    }

    public Point getMidPoint() {
        return midPoint;
    }

    public void setMidPoint(Point midPoint) {
        this.midPoint = midPoint;
    }


    public Octree(String indexName, Vector<NameAndType> nameAndType , Point min, Point max, int maxNumberOfPoints) {
        this.indexName = indexName;
        this.maxNumberOfPoints = maxNumberOfPoints;
        points = new Vector<>();
        children = new Vector<>();
        maxPoint = max;
        minPoint = min;
        midPoint = getMidValue(min, max);
        this.nameAndType= new Vector<>();
        for(int i=0;i<nameAndType.size();i++){
            this.nameAndType.add(new NameAndType(nameAndType.get(i).getColomnName(), nameAndType.get(i).getColomnType()));
        }
     
    }

    public Octree(String indexName, Vector<NameAndType>nameAndType, int maxNumberOfPoints) {
        this.indexName = indexName;
        this.maxNumberOfPoints = maxNumberOfPoints;
        this.nameAndType= new Vector<>();
        for(int i=0;i<nameAndType.size();i++){
            this.nameAndType.add(new NameAndType(nameAndType.get(i).getColomnName(), nameAndType.get(i).getColomnType()));
        }
        points = new Vector<>();
        children = new Vector<>();
       
    }

//    public Octree(String indexName, Hashtable<String, String> nameAndType, References p, Point max, Point min) {
//        this.nameAndType = nameAndType;
//        this.indexName = indexName;
//        maxNumberOfPoints = getMaxEntriesOctreeNode();
//        points = new Vector();
//        children = new Vector<>();
//        points.add(p);
//        maxPoint = max;
//        minPoint = min;
//        midPoint = getMidValue(min, max);
//    }

    public void insert(Point p, String ref, String tableName, Object clusteringKey) {
    		
        if (children.isEmpty() && points.size() < maxNumberOfPoints) {
            points.add(new References(p));
            points.get(points.size() - 1).getPageref().add(ref);
            points.get(points.size() - 1).getKeyValue().add(clusteringKey);
//            if (genericCompare(p.getX(),maxPoint.getX()) > 0) {
//                maxPoint.setX(p.getX());
//            }
//            if (genericCompare(p.getY(),maxPoint.getY()) > 0) {
//                maxPoint.setY(p.getY());
//            }
//            if (genericCompare(p.getZ(),maxPoint.getZ()) > 0) {
//                maxPoint.setZ(p.getZ());
//            }
//            if (genericCompare(minPoint.getX(),p.getX()) > 0) {
//                minPoint.setX(p.getX());
//            }
//            if (genericCompare(minPoint.getY(),p.getY()) > 0) {
//                minPoint.setY(p.getY());
//            }
//            if (genericCompare(minPoint.getZ(),p.getZ()) > 0) {
//                minPoint.setZ(p.getZ());
//            }

            return;
        }
       
        if (children.isEmpty() && points.size() == maxNumberOfPoints) {
            for (int i = 0; i < 8; i++) {
                Octree child = new Octree(indexName, nameAndType, maxNumberOfPoints);
                this.children.add(child);
            }
            children.get(0).setMinPoint(this.minPoint);
            children.get(0).setMaxPoint(this.midPoint);
            children.get(0).setMidPoint(getMidValue(children.get(0).getMinPoint(), children.get(0).getMaxPoint()));
            children.get(1).setMinPoint(new Point(midPoint.getX(), minPoint.getY(), minPoint.getZ()));
            children.get(1).setMaxPoint(new Point(maxPoint.getX(), midPoint.getY(), midPoint.getZ()));
            children.get(1).setMidPoint(getMidValue(children.get(1).getMinPoint(), children.get(1).getMaxPoint()));
            children.get(2).setMinPoint(new Point(midPoint.getX(), midPoint.getY(), minPoint.getZ()));
            children.get(2).setMaxPoint(new Point(maxPoint.getX(), maxPoint.getY(), midPoint.getZ()));
            children.get(2).setMidPoint(getMidValue(children.get(2).getMinPoint(), children.get(2).getMaxPoint()));
            children.get(3).setMinPoint(new Point(minPoint.getX(), midPoint.getY(), minPoint.getZ()));
            children.get(3).setMaxPoint(new Point(midPoint.getX(), maxPoint.getY(), midPoint.getZ()));
            children.get(3).setMidPoint(getMidValue(children.get(3).getMinPoint(), children.get(3).getMaxPoint()));
            children.get(4).setMinPoint(new Point(minPoint.getX(), minPoint.getY(), midPoint.getZ()));
            children.get(4).setMaxPoint(new Point(midPoint.getX(), midPoint.getY(), maxPoint.getZ()));
            children.get(4).setMidPoint(getMidValue(children.get(4).getMinPoint(), children.get(4).getMaxPoint()));
            children.get(5).setMinPoint(new Point(midPoint.getX(), minPoint.getY(), midPoint.getZ()));
            children.get(5).setMaxPoint(new Point(maxPoint.getX(), midPoint.getY(), maxPoint.getZ()));
            children.get(5).setMidPoint(getMidValue(children.get(5).getMinPoint(), children.get(5).getMaxPoint()));
            children.get(6).setMinPoint(midPoint);
            children.get(6).setMaxPoint(maxPoint);
            children.get(6).setMidPoint(getMidValue(children.get(6).getMinPoint(), children.get(6).getMaxPoint()));
            children.get(7).setMinPoint(new Point(minPoint.getX(), midPoint.getY(), midPoint.getZ()));
            children.get(7).setMaxPoint(new Point(midPoint.getX(), maxPoint.getY(), maxPoint.getZ()));
            children.get(7).setMidPoint(getMidValue(children.get(7).getMinPoint(), children.get(7).getMaxPoint()));

            Vector<References> parentPoints = new Vector<>();
            for (References current : points) {
                parentPoints.add(current);
            }
     //       References refff=new References(p);
      //      refff.getPageref().add(ref);
     //       refff.getKeyValue().add(clusteringKey);
            
            this.points.removeAllElements();
     //       parentPoints.add(refff);
            for (References current : parentPoints) {
                for (int i = 0; i < current.getPageref().size(); i++) {
                    this.insert(current.getP(), current.getPageref().get(i), tableName, current.getKeyValue().get(i));
                  
                }
            }
       
            this.insert(p, ref, tableName, clusteringKey);
            return;
        }
        if (!children.isEmpty()) {
            if (genericCompare(p.getX(), midPoint.getX()) <= 0) {
                if (genericCompare(p.getY(), midPoint.getY()) <= 0) {
                    if (genericCompare(p.getZ(), midPoint.getZ()) <= 0) {
                        children.get(0).insert(p, ref, tableName, clusteringKey);
                    }
                    else
                        children.get(4).insert(p, ref, tableName, clusteringKey);
                    	
                } else {
                    if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                        children.get(3).insert(p, ref, tableName, clusteringKey);
                    else
                        children.get(7).insert(p, ref, tableName, clusteringKey);
                }
            } else {
                if (genericCompare(p.getY(), midPoint.getY()) <= 0) {
                    if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                        children.get(1).insert(p, ref, tableName, clusteringKey);
                    else 
                        children.get(5).insert(p, ref, tableName, clusteringKey);
                } else {
                    if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                        children.get(2).insert(p, ref, tableName, clusteringKey);
                    else 
                        children.get(6).insert(p, ref, tableName, clusteringKey);
                }
            }
        }
    }

    public void updatePoint(Point p,String ref,Object ClustringKey) {
    	References z=Search(p);
    	for(int i=0;i<z.getPageref().size();i++) {
    		if(genericCompare(z.getKeyValue().get(i),ClustringKey)==0) {
    			z.getPageref().remove(i);
    			z.getPageref().add(i, ref);
    		}
    	}
    }
    public void deleteFromOctree(Point p,Object clusteringKey){
    	Point midPoint = getMidValue(minPoint, maxPoint);
        
        int pos = -1;
        if(this.isEmptyOctree()){
        	System.out.print("empty");
        	return ;
        }
        if(Search(p)==null){
        	System.out.print("search");
        	return;
        }
        if(!(this.points.size()==0) ){
        	for(References child: points ){
         		if(genericCompare(child.getP().getX(),p.getX())==0 &&genericCompare(p.getY(),child.getP().getY())== 0 &&genericCompare(p.getZ(), child.getP().getZ()) == 0){
         		if(clusteringKey==null){
        	     this.points.remove(child);
        	     return;
        	     }
         		else{
         			System.out.println("else");
         			for(int k=0;k<child.getKeyValue().size();k++){
         				System.out.println("for");
         				if(genericCompare(child.getKeyValue().get(k),clusteringKey)==0 
         						){
         					child.getKeyValue().remove(k);
         				    child.getPageref().remove(k);
         					if (child.getKeyValue().size()==0){
         						this.points.remove(child);
         					}
         						   
         				    return;
         		}
         				}
         		
        		}
         		return;
        	
        }}}
        
        // Deciding the position
        // where to move
        if (genericCompare(p.getX(), midPoint.getX()) <= 0) {
            if (genericCompare(p.getY(), midPoint.getY()) <= 0) {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 0;
                else
                    pos = 4;
            } else {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 3;
                else
                    pos = 7;
            }
        } else {
            if (genericCompare(p.getY(), midPoint.getY()) <= 0) {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 1;
                else
                    pos = 5;
            } else {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 2;
                else
                    pos = 6;
            }
        }
        //System.out.println(pos);
        
        if(pos==-1)
        	return;
        else{
          children.get(pos).deleteFromOctree(p,clusteringKey);
         boolean flag=false;
          for(int i=0;i<8;i++){
        	  if(!children.get(i).getPoints().isEmpty() || !children.get(i).isEmptyOctree())
        		  flag=true;}
          if(!flag){
        	  children.removeAllElements();
          }
          return;
        }
    }
    
    public void updateoctree(Point p1, Point p2 , Object clusteringKey,String tableName ){
    	System.out.println("method");
    if (this.Search(p1)==null){
    	return;
    }
    	References a = new References(p1) ;
    	for (int j=0;j<this.Search(p1).getPageref().size();j++){
    		a.getPageref().add(this.Search(p1).getPageref().get(j));
    		a.getKeyValue().add(this.Search(p1).getKeyValue().get(j));
    		
    		
    	}
    
    		for (int i=0;i<a.getKeyValue().size();i++){
    			
				if (genericCompare (a.getKeyValue().get(i),clusteringKey)==0){
				    System.out.println("HIIIII");
					this.deleteFromOctree(p1, clusteringKey);
					
  			        this.insert(p2, a.getPageref().get(i), tableName, clusteringKey);
  			 
  			         return;
  			}
			}
   }
    public void updateOctreeDuplicates(Point p, String ref, String tableName, Object clusteringKey){
    	Point midPoint = getMidValue(minPoint, maxPoint);

        int pos = -1;
        if(this.isEmptyOctree()){
        	System.out.print("Octree empty");
        	return;
        }
        if(!(this.points.size()==0) ){
        	for(References child: points ){
         		if(genericCompare(child.getP().getX(),p.getX())==0 &&genericCompare(p.getY(),child.getP().getY())== 0 &&genericCompare(p.getZ(), child.getP().getZ()) == 0){
        	     //System.out.println("kimo");
         	    child.getKeyValue().add(clusteringKey);
         		child.getPageref().add(ref);
         		//System.out.println(child);
        		}}
        	return;
        }
        
        // Deciding the position
        // where to move
        if (genericCompare(p.getX(), midPoint.getX()) <= 0) {
            if (genericCompare(p.getY(), midPoint.getY()) <= 0) {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 0;
                else
                    pos = 4;
            } else {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 3;
                else
                    pos = 7;
            }
        } else {
            if (genericCompare(p.getY(), midPoint.getY()) <= 0) {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 1;
                else
                    pos = 5;
            } else {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 2;
                else
                    pos = 6;
            }
        }
   
        
        if(pos==-1)
        	return ;
        else
         children.get(pos).updateOctreeDuplicates(p,ref,tableName,clusteringKey);
        
    	
    }
    public static int genericCompare(Object a, Object b) {
    	//System.out.println(a+" " +b);
    	
        if (a  instanceof Integer && b instanceof Integer)
            return ((Integer) a).compareTo((Integer) b);
        else if (a instanceof Double && b instanceof Double)
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
        } else if (a instanceof String && b instanceof String)
            return ((String) a).compareTo((String) b);
        else
            return 0;
    }

    public Point getMidValue(Point p1, Point p2) {
    	
        return new Point(getMid(p1.getX(), p2.getX()), getMid(p1.getY(), p2.getY()), getMid(p1.getZ(), p2.getZ()));
    }

    public Object getMid(Object x, Object y) {
        if (x instanceof Integer && y instanceof Integer)
            return ((int) x + (int) y) / 2;
        else if (x instanceof Double && y instanceof Double)
            return ((double) x + (double) y) / 2;
        else if (x instanceof String && y instanceof String)
            return getMiddleString((String) x, (String) y);
        else if (x instanceof Date && y instanceof Date) {
            Date d1 = (Date) x;
            Date d2 = (Date) y;
            long timeBetween;
            long midpointTime;
            if (genericCompare(d2, d1) > 0) {
                timeBetween = d2.getTime() - d1.getTime();
                midpointTime = d1.getTime() + (timeBetween / 2);
            } else {
                timeBetween = d1.getTime() - d2.getTime();
                midpointTime = d2.getTime() + (timeBetween / 2);
            }
            Date midpoint = new Date(midpointTime);
            return midpoint;
        } else return null;
    }

  /*  public void DeleteFromOctree(Point p, int pageNumber, String tableName) {
        for (int i = 0; i < Search(p).size(); i++) {
            String r = "./src/resources/tables/" + tableName + "/page" + pageNumber + ".ser";
            for (int j = 0; j < Search(p).get(i).getPageref().size(); j++) {
                if (genericCompare(Search(p).get(i).getP().getX(), p.getX()) == 0 && genericCompare(Search(p).get(i).getP().getY(), p.getY()) == 0
                        && genericCompare(Search(p).get(i).getP().getZ(), p.getZ()) == 0 &&
                        Search(p).get(i).getPageref().get(j).equals(r))
                    Search(p).remove(i);
            }
        }*/



    public static String getMiddleString(String S, String T) {
        int N;
        String res = "";
        // Stores the base 26 digits after addition

        if (S.length() > T.length()) {
            for (int i = T.length(); i < S.length(); i++)
                T += S.charAt(i);
        }
        if (T.length() > S.length()) {
            for (int i = S.length(); i < T.length(); i++)
                S += T.charAt(i);
        }
        N = T.length();

        Vector<Integer> a1 = new Vector(N + 1);
        for (int i = 0; i < N; i++) {
            a1.add(i, (S.charAt(i) - 'a' + T.charAt(i) - 'a'));
        }

        // Iterate from right to left
        // and add carry to next position
        for (int i = N - 1; i >= 1; i--) {
            a1.set(i - 1, a1.get(i - 1) + (a1.get(i) / 26));
            a1.set(i, a1.get(i) % 26);
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i < N; i++) {

            // If current value is odd,
            // carry 26 to the next index value
            if (a1.get(i) % 2 == 1) {

                if (i + 1 < N) {
                    a1.set(i + 1, a1.get(i + 1) + 26);
                }
            }

            a1.set(i, a1.get(i) / 2);
        }

        for (int i = 0; i < N; i++) {
            res += ((char) (a1.get(i) + 'a'));
        }
        return res;


    }
//    public static String getMiddlePoint(String str1, String str2) {
//        int length1 = str1.length();
//        int length2 = str2.length();
//        int middle = (length1 + length2) / 2;
//        String middleSubstring;
//        if (length1 <= length2) {
//            middleSubstring = str1.substring(length1/2) + str2.substring(0, middle - length1/2);
//        } else {
//            middleSubstring = str2.substring(length2/2) + str1.substring(0, middle - length2/2);
//        }
//        return middleSubstring;
//    }

//    public static String getMiddleString(String s1, String s2) {
//        int totalLength = s2.length() - s1.length() + 1;
//        int middleIndex = s1.length() + (totalLength / 2);
//
//        StringBuilder sb = new StringBuilder();
//        if (middleIndex <= s1.length()) {
//            sb.append(s1.charAt(middleIndex));
//        } else if (middleIndex == s1.length() + 1) {
//            sb.append(s1.charAt(s1.length() - 1));
//            sb.append(s2.charAt(0));
//        } else {
//            sb.append(s2.charAt(middleIndex - s1.length() - 1));
//        }
//
//        return sb.toString();
//    }

    public Vector<Octree> getChildren() {
        return this.children;
    }


    public References Search(Point p) {
        // If point is out of bound

        // Otherwise perform binary search
        // for each ordinate
        Point midPoint = getMidValue(minPoint, maxPoint);

        int pos = -1;
        if(this.isEmptyOctree()){
        	return null;
        }
        if(!(this.points.size()==0) ){
        	for(References child: points ){
         		if(genericCompare(child.getP().getX(),p.getX())==0 &&genericCompare(p.getY(),child.getP().getY())== 0 &&genericCompare(p.getZ(), child.getP().getZ()) == 0)
        	     return child;
        		}
        	return null;
        }
        
        // Deciding the position
        // where to move
        if (genericCompare(p.getX(), midPoint.getX()) <= 0) {
            if (genericCompare(p.getY(), midPoint.getY()) <= 0) {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 0;
                else
                    pos = 4;
            } else {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 3;
                else
                    pos = 7;
            }
        } else {
            if (genericCompare(p.getY(), midPoint.getY()) <= 0) {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 1;
                else
                    pos = 5;
            } else {
                if (genericCompare(p.getZ(), midPoint.getZ()) <= 0)
                    pos = 2;
                else
                    pos = 6;
            }
        }
        //System.out.println(pos);
        
        if(pos==-1)
        	return null;
        else{
         return children.get(pos).Search(p);
        }
      
    }

    public boolean isEmptyOctree(){
    	if(points.isEmpty() && children.isEmpty())
    		return true;
    	else
    		return false;
    	
    }
    
    public void printOctree() {
        printOctree(this, 0);
    }

    private void printOctree(Octree node, int depth) {
        if (node == null) {
            return;
        }

        if (depth == 0) System.out.println(indexName);

        for (int i = 0; i < depth; i++) {
            System.out.print(" _ ");
        }
        System.out.println(node);

        for (Octree child : node.children) {
            printOctree(child, depth + 1);
        }
    }

    public String toString() {
        String out = "(";
        for (References p : points) {
            out += p + ", ";
        }
        out += ")";
        return out;
    }

    public Vector <NameAndType>getNameAndType() {
        return nameAndType;
    }

    public void setNameAndType(Vector<NameAndType> nameAndType) {
        this.nameAndType = nameAndType;
    }

    public Vector<References> getPoints() {
        return points;
    }

    public void setPoints(Vector<References> points) {
        this.points = points;
    }

    public int getMaxNumberOfPoints() {
        return maxNumberOfPoints;
    }

    public void setMaxNumberOfPoints(int maxNumberOfPoints) {
        this.maxNumberOfPoints = maxNumberOfPoints;
    }

    public void setChildren(Vector<Octree> children) {
        this.children = children;
    }
   
    
}
