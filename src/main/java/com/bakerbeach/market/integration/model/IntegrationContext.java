package com.bakerbeach.market.integration.model;

import java.util.Currency;
import java.util.List;
import java.util.Locale;

public interface IntegrationContext {

	String getShopCode();

	List<Locale> getLocales();

	String getCurrencySymbol();

	List<String> getPriceGroups();

	String getAssortmentCode();

	String getLiveCollectionCode(String status);

	List<Currency> getCurrencies();

}