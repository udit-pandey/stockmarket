package dto;

import java.io.Serializable;

public class StockVolume implements Serializable {
    private double volume;

    public StockVolume(double volume) {
        this.volume = volume;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    @Override
    public String toString() {
        return "volume=" + volume;
    }
}
