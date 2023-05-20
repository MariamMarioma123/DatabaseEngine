import java.io.Serializable;

public class NameAndType implements Serializable {
    private String colomnName;
    private String colomnType;


    public NameAndType(String colomnName,String colomnType){
        this.colomnName=colomnName;
        this.colomnType=colomnType;
    }

    public String getColomnName() {
        return colomnName;
    }

    public void setColomnName(String colomnName) {
        this.colomnName = colomnName;
    }

    public String getColomnType() {
        return colomnType;
    }

    public void setColomnType(String colomnType) {
        this.colomnType = colomnType;
    }
}
