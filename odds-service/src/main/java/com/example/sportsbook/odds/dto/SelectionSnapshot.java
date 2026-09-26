package com.example.sportsbook.odds.dto;

import java.math.BigDecimal;

public record SelectionSnapshot(String selectionId, BigDecimal odds) {
}
