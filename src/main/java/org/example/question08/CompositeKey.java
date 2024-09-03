package org.example.question08;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CompositeKey implements WritableComparable<CompositeKey> {

    private String year;
    private String country;

    public CompositeKey() {}

    public CompositeKey(String year, String country) {
        this.year = year;
        this.country = country;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeKey that = (CompositeKey) o;
        return Objects.equals(year, that.year) && Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, country);
    }

    @Override
    public int compareTo(CompositeKey o) {
        int result = this.year.compareTo(o.year);
        if (result == 0) {
            result = this.country.compareTo(o.country);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(year);
        dataOutput.writeUTF(country);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        year = dataInput.readUTF();
        country = dataInput.readUTF();

    }

}
