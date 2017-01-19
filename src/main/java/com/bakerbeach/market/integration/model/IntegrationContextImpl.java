package com.bakerbeach.market.integration.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Currency;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.LocaleUtils;

public class IntegrationContextImpl implements IntegrationContext {
	private String shopCode;
	private List<Locale> locales;
	private String currencySymbol;
	private List<Currency> currencies;
	private List<String> priceGroups;
	private String assortmentCode;

	@Override
	public String getShopCode() {
		return shopCode;
	}

	public void setShopCode(String shopCode) {
		this.shopCode = shopCode;
	}

	@Override
	public List<Locale> getLocales() {
		return locales;
	}

	public void setLocales(List<Locale> locales) {
		this.locales = locales;
	}

	public void setLocalesString(String localesStr) {
		this.locales = new ArrayList<Locale>();
		for (String str : localesStr.split(",")) {
			this.locales.add(LocaleUtils.toLocale(str));
		}
	}

	@Override
	public String getCurrencySymbol() {
		return currencySymbol;
	}

	public void setCurrencySymbol(String currencySymbol) {
		this.currencySymbol = currencySymbol;
	}

	@Override
	public List<Currency> getCurrencies() {
		return currencies;
	}

	public void setCurrencies(List<Currency> currencies) {
		this.currencies = currencies;
	}

	public void setCurrenciesString(String currenciesStr) {
		this.currencies = new ArrayList<Currency>();
		for (String str : currenciesStr.split(",")) {
			this.currencies.add(Currency.getInstance(str));
		}
	}

	@Override
	public List<String> getPriceGroups() {
		return priceGroups;
	}

	public void setPriceGroups(List<String> priceGroups) {
		this.priceGroups = priceGroups;
	}

	public void setPriceGroupsString(String priceGroupsStr) {
		setPriceGroups(Arrays.asList(priceGroupsStr.split(",")));
	}

	public void setAssortmentCode(String assortmentCode) {
		this.assortmentCode = assortmentCode;
	}

	@Override
	public String getAssortmentCode() {
		return assortmentCode;
	}

	@Override
	public String getLiveCollectionCode(String status) {
		if ("PUBLISHED".equals(status) || "WORK".equals(status)) {
			StringBuilder sb = new StringBuilder(assortmentCode.trim().toUpperCase()).append("_")
					.append(status.trim().toUpperCase());
			return sb.toString();
		}
		return null;
	}

}
