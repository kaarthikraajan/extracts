package com.tivo.extracts;

import java.math.BigDecimal;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Trade {

	private String isin;
	private long quantity;
	private BigDecimal price;
	private String customer;

	// getters and setters omitted

}