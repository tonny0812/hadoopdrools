package com.keepgulp.hadoopdrools.model;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class Transaction {

    private String creditCard;
    private String city;
    private String state;
    private double amount;

    public Transaction(String row) {
        String[] parts = row.split(",");

        if (parts.length != 4) {
            throw new IllegalArgumentException("Row incorrectly formatted!");
        }

        creditCard = parts[0];
        city = parts[1];
        state = parts[2];
        amount = Double.parseDouble(parts[3]);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(amount);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + ((city == null) ? 0 : city.hashCode());
        result = prime * result
                + ((creditCard == null) ? 0 : creditCard.hashCode());
        result = prime * result + ((state == null) ? 0 : state.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Transaction other = (Transaction) obj;
        if (Double.doubleToLongBits(amount) != Double
                .doubleToLongBits(other.amount))
            return false;
        if (city == null) {
            if (other.city != null)
                return false;
        } else if (!city.equals(other.city))
            return false;
        if (creditCard == null) {
            if (other.creditCard != null)
                return false;
        } else if (!creditCard.equals(other.creditCard))
            return false;
        if (state == null) {
            if (other.state != null)
                return false;
        } else if (!state.equals(other.state))
            return false;
        return true;
    }
    @Override
    public String toString() {
        return creditCard + "," + city + "," + state + "," + amount;
    }
}
