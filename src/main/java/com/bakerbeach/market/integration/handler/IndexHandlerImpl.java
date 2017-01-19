
package com.bakerbeach.market.integration.handler;

import java.util.ArrayList;
import java.util.Currency;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.processor.aggregate.AggregationStrategy;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.bakerbeach.market.catalog.model.RawProduct;
import com.bakerbeach.market.catalog.service.CatalogRawDataService;
import com.bakerbeach.market.index.service.IndexService;
import com.bakerbeach.market.integration.model.IntegrationContext;

public class IndexHandlerImpl extends AbstractHandler implements AggregationStrategy {
	protected static final Logger log = LoggerFactory.getLogger(IndexHandlerImpl.class);
	private static final Integer DEFAULT_GTIN_CHUNK_SIZE = 20;
	private static Integer gtinChunkSize = DEFAULT_GTIN_CHUNK_SIZE;
	
	@Autowired
	private CatalogRawDataService catalogService;
	@Autowired
	private IndexService indexService;

	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> updateAll(Exchange ex) {
		try {
			Map<String, Object> payload = (Map<String, Object>) getPayload(ex.getIn());
			String shop = (String) payload.get("shop");
			String status = (String) payload.get("status");
			Date lastUpdate = (payload.containsKey("lastUpdate")) ? (Date) payload.get("lastUpdate") : new Date();

			List<String> gtins = catalogService.findGtin(status, true);

			List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
			for (Iterator<String> i = gtins.iterator(); i.hasNext();) {
				List<String> gtin = new ArrayList<String>();

				for (int j = 0; j < gtinChunkSize && i.hasNext(); j++) {
					gtin.add(i.next());
				}

				Map<String, Object> body = new HashMap<String, Object>(4);
				body.put("shop", shop);
				body.put("status", status);
				body.put("lastUpdate", lastUpdate);
				body.put("gtin", gtin);

				out.add(body);
			}

			return out;
		} catch (EventHandlerException e) {
			log.error(ExceptionUtils.getStackTrace(e));
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public void index(Exchange ex) {
		try {
			log.info("index : " + ex.getIn().getBody());

			Map<String, Object> payload = (Map<String, Object>) getPayload(ex.getIn());
			String shop = (String) payload.get("shop");
			String status = (String) payload.get("status");
			Date lastUpdate = (payload.containsKey("lastUpdate")) ? (Date) payload.get("lastUpdate") : new Date();
			List<String> gtin = (List<String>) payload.get("gtin");

			IntegrationContext context = contextMap.get(shop);
			List<Locale> locales = context.getLocales();
			List<Currency> currencies = context.getCurrencies();
			List<String> priceGroups = context.getPriceGroups();

			List<RawProduct> products = catalogService.findByGtin(status, gtin);

			indexService.index(products, shop, status, lastUpdate, locales, currencies, priceGroups);
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		}
	}

	public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
		log.info("aggregate : " + newExchange.getIn().getBody());
		return newExchange;
	}

	public void afterUpdate(Exchange ex) {
		try {
			log.info("afterUpdate : " + ex.getIn().getBody());
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		}
	}

	public static void setGtinChunkSize(Integer gtinChunkSize) {
		IndexHandlerImpl.gtinChunkSize = gtinChunkSize;
	}
	
	public CatalogRawDataService getCatalogService() {
		return catalogService;
	}

	public void setCatalogService(CatalogRawDataService catalogService) {
		this.catalogService = catalogService;
	}

	/**
	 * @return the indexService
	 */
	public IndexService getIndexService() {
		return indexService;
	}

	/**
	 * @param indexService the indexService to set
	 */
	public void setIndexService(IndexService indexService) {
		this.indexService = indexService;
	}

}
