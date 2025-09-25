package nextflow.fovus.nio;

import java.util.Date;
import java.util.Objects;

/**
 * Represents metadata for a file in Fovus storage as a result of a list objects operation.
 *
 * The file metadata can be used to create a {@link FovusFileAttributes} object.
 */
public class FovusFileMetadata {

    private String key;
    private Date lastModified;          // <-- now using java.util.Date
    private String eTag;
    private long size;

    public FovusFileMetadata() {
        // no-args constructor
    }

    public FovusFileMetadata(String key,
                             Date lastModified,
                             String eTag,
                             long size) {
        this.key = key;
        this.lastModified = lastModified;
        this.size = size;
        this.eTag = eTag;
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

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getETag() {
        return eTag;
    }

    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    @Override
    public String toString() {
        return "FovusFileMetadata {" +
                "key='" + key + '\'' +
                ", lastModified=" + lastModified +
                ", size=" + size +
                ", eTag='" + eTag + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FovusFileMetadata)) return false;
        FovusFileMetadata that = (FovusFileMetadata) o;
        return size == that.size &&
                Objects.equals(key, that.key) &&
                Objects.equals(lastModified, that.lastModified) &&
                Objects.equals(eTag, that.eTag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, lastModified, size, eTag);
    }
}
