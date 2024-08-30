package org.example.question07;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AverageWritable implements Writable {

    private float price;
    private int n;

    public AverageWritable() {}

    public AverageWritable(float price, int n) {

        this.price = price;
        this.n = n;

    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(String.valueOf(price));
        dataOutput.writeUTF(String.valueOf(n));

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        price = Float.parseFloat(dataInput.readUTF());
        n = Integer.parseInt(dataInput.readUTF());

    }
}
