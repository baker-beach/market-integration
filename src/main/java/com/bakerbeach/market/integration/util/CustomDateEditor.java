package com.bakerbeach.market.integration.util;

import java.text.SimpleDateFormat;

public class CustomDateEditor extends org.springframework.beans.propertyeditors.CustomDateEditor {

	public CustomDateEditor() {
		super(new SimpleDateFormat("yyyy-MM-dd"), true);
	}

}
