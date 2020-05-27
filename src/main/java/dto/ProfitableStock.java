package dto;

import java.io.Serializable;

public class ProfitableStock implements Serializable {
    private double closingPriceSum;
    private double openingPriceSum;
    private int count;

    public ProfitableStock(double closingPriceSum, double openingPriceSum, int count) {
        this.closingPriceSum = closingPriceSum;
        this.openingPriceSum = openingPriceSum;
        this.count = count;
    }

    public double getClosingPriceSum() {
        return closingPriceSum;
    }

    public void setClosingPriceSum(double closingPriceSum) {
        this.closingPriceSum = closingPriceSum;
    }

    public double getOpeningPriceSum() {
        return openingPriceSum;
    }

    public void setOpeningPriceSum(double openingPriceSum) {
        this.openingPriceSum = openingPriceSum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getProfit() {
        double avgClosingPrice = closingPriceSum / count;
        double avgOpeningPrice = openingPriceSum / count;
        return avgClosingPrice - avgOpeningPrice;
    }

    @Override
    public String toString() {
        double avgClosingPrice = closingPriceSum / count;
        double avgOpeningPrice = openingPriceSum / count;
        return Double.toString(avgClosingPrice - avgOpeningPrice);
    }
}
