package cz.cas.lib.arcstorage.exception;

public class MissingObject extends GeneralException {
    private String type;

    private String id;

    public MissingObject(Class clazz, String id) {
        super();
        this.type = clazz.getTypeName();
        this.id = id;
    }

    public MissingObject(String type, String id) {
        super();
        this.type = type;
        this.id = id;
    }

    @Override
    public String toString() {
        return "MissingObject{" +
                "type=" + type +
                ", id='" + id + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }
}
