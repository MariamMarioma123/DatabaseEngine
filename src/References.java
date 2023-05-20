import java.io.Serializable;
import java.util.Vector;


public class References implements Serializable {
    Point p;
    Vector<String> pageref = new Vector();
    Vector<Object> keyValue = new Vector();

    public References(Point p) {
        //this.p=new ()
        //this.p.setX(p.getX());
        //this.p.setY(p.getY());
        //this.p.setZ(p.getZ());
        this.p = p;
        
        
    }

    public Point getP() {
        return p;
    }

    public void setP(Point p) {
        this.p = p;
    }

    public Vector<String> getPageref() {
        return pageref;
    }

    public void setPageref(Vector<String> pageref) {
        this.pageref = pageref;
    }

    public Vector<Object> getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(Vector<Object> keyValue) {
        this.keyValue = keyValue;
    }

    public String toString() {
        String r="["+ p +"Keys:";
        for(int i=0;i<keyValue.size();i++ ){
        	r+= " "+ keyValue.get(i)+ " ";
        }
        r+=pageref+ " ]";
        return r;
    }
    

}
