package nextflow.fovus.nio;

import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Represents metadata for an S3-like object.
 */
public class ObjectMetaData {

    private String key;
    private Date lastModified;          // <-- now using java.util.Date
    private String eTag;
    private List<String> checksumAlgorithm;
    private String checksumType;
    private long size;
    private String storageClass;

    public ObjectMetaData() {
        // no-args constructor
    }

    public ObjectMetaData(String key,
                          Date lastModified,
                          String eTag,
                          List<String> checksumAlgorithm,
                          String checksumType,
                          long size,
                          String storageClass) {
        this.key = key;
        this.lastModified = lastModified;
        this.eTag = eTag;
        this.checksumAlgorithm = checksumAlgorithm;
        this.checksumType = checksumType;
        this.size = size;
        this.storageClass = storageClass;
    }

    // Getters and setters
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public String getETag() {
        return eTag;
    }

    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    public List<String> getChecksumAlgorithm() {
        return checksumAlgorithm;
    }

    public void setChecksumAlgorithm(List<String> checksumAlgorithm) {
        this.checksumAlgorithm = checksumAlgorithm;
    }

    public String getChecksumType() {
        return checksumType;
    }

    public void setChecksumType(String checksumType) {
        this.checksumType = checksumType;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    @Override
    public String toString() {
        return "ObjectMetaData{" +
                "key='" + key + '\'' +
                ", lastModified=" + lastModified +
                ", eTag='" + eTag + '\'' +
                ", checksumAlgorithm=" + checksumAlgorithm +
                ", checksumType='" + checksumType + '\'' +
                ", size=" + size +
                ", storageClass='" + storageClass + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ObjectMetaData)) return false;
        ObjectMetaData that = (ObjectMetaData) o;
        return size == that.size &&
                Objects.equals(key, that.key) &&
                Objects.equals(lastModified, that.lastModified) &&
                Objects.equals(eTag, that.eTag) &&
                Objects.equals(checksumAlgorithm, that.checksumAlgorithm) &&
                Objects.equals(checksumType, that.checksumType) &&
                Objects.equals(storageClass, that.storageClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, lastModified, eTag, checksumAlgorithm, checksumType, size, storageClass);
    }
}
