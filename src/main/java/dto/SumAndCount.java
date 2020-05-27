package dto;

import java.io.Serializable;

public class SumAndCount implements Serializable {
    private double sum;
    int count;

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return Double.toString(sum / count);
    }
}
