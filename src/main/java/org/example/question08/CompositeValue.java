package org.example.question08;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeValue implements Writable {

    private long price;
    private float quantity;

    public CompositeValue() {}

    public CompositeValue(long price, float quantity) {
        this.price = price;
        this.quantity = quantity;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public float getQuantity() {
        return quantity;
    }

    public void setQuantity(float quantity) {
        this.quantity = quantity;
    }

    public float getTotal() {

        return price * quantity;

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(String.valueOf(price));
        dataOutput.writeUTF(String.valueOf(quantity));

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        price =  Long.parseLong(dataInput.readUTF());
        quantity =  Float.parseFloat(dataInput.readUTF());

    }
}
