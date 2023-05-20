import java.io.Serializable;

public class Point implements Serializable {
	private Object x;
	private Object y;
	private Object z;



	public Point(Object x, Object y, Object z){
		this.x=x;
		this.y=y;
		this.z=z;
	}
	
    public Point(){
    	this.x=null;
    	this.y=null;
    	this.z=null;
    }

	public void setX(Object x) {
		this.x = x;
	}

	public void setY(Object y) {
		this.y = y;
	}

	public void setZ(Object z) {
		this.z = z;
	}

	public Object getX(){
		return x;
	}
	
	public Object getY(){
		return y;
	}
	public Object getZ(){
		return z;
	}

	@Override
	public String toString() {
		return "[" +
				"x=" + x +
				", y=" + y +
				", z=" + z +
				"]";
	}
}
