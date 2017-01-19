package com.bakerbeach.market.integration.service;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.bakerbeach.market.core.api.model.Asset;
import com.bakerbeach.market.core.api.model.AssetGroup;
import com.bakerbeach.market.core.api.model.Assets;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

import au.com.bytecode.opencsv.CSVReader;

public class CatalogIntegrationService {
	protected static final Logger log = LoggerFactory.getLogger(CatalogIntegrationService.class);

	private MongoTemplate mongoCmsTemplate;
	private MongoTemplate mongoShopTemplate;
	private Map<String, DBCollection> collectionMap = new HashMap<String, DBCollection>();

	private boolean importMessages = true;
	private boolean importUrl = false;
	private boolean importSimpleProduct = false;
	private boolean importSimpleProductInventory = false;
	private boolean importGroupProduct = false;
	private boolean importSimpleProductRecommendations = true;

	private DBCollection getCollection(String collectionName, MongoTemplate mongoTemplate) {
		if (!collectionMap.containsKey(collectionName))
			collectionMap.put(collectionName, mongoTemplate.getCollection(collectionName));
		return collectionMap.get(collectionName);
	}

	public void csvProductUpdate(String productCsvPath) {
		try {

			CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(productCsvPath)), ';', '\"', CSVReader.DEFAULT_ESCAPE_CHARACTER);
			List<String[]> list = reader.readAll();
			reader.close();

			String[] keys = list.get(0);
			list.remove(0);

			Map<String, List<Map<String, String>>> groups = new HashMap<String, List<Map<String, String>>>();
			for (String[] values : list) {
				try {
					Map<String, String> productMap = toMap(keys, values);
					
					if (!groups.containsKey(productMap.get("Grouped Products").substring(0, 4))) {
						List<Map<String, String>> groupList = new ArrayList<Map<String, String>>();
						groups.put(productMap.get("Grouped Products").substring(0, 4), groupList);
					}
					groups.get(productMap.get("Grouped Products").substring(0, 4)).add(productMap);					
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}

			for (String groupGtin : groups.keySet()) {
				importGroupProduct(groups.get(groupGtin), groupGtin);
			}

		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		}

	}

	void insertMessage(BasicDBObject message) {
		if (importMessages) {
			QueryBuilder qb = QueryBuilder.start();
			qb.and("type").is(message.get("type"));
			qb.and("tag").is(message.get("tag"));
			qb.and("code").is(message.get("code"));
			getCollection("messages", mongoCmsTemplate).update(qb.get(), message, true, false);
		}
	}

	private void createProductMessages(Map<String, String> productMap, String code) {

		BasicDBObject message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.code");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Product Name  ")));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.description1");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Description (paragraph)")));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.description2");
		message.put("translations", new BasicDBObject("en_GB", createBulletPoints(productMap.get("Description (bullet points)"))));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.material");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Material")));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.technology");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Technology")));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.care");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Care")));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);
		message.put("tag", "product.cart.title1");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Product Name  ")));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);
		message.put("tag", "product.cart.title2");
		message.put("translations", new BasicDBObject("en_GB", "Style:&nbsp;" + productMap.get("Style Number [SKU number]") + " (" + code + ")"));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);
		message.put("tag", "product.cart.title3");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Colour")));
		insertMessage(message);

	}

	private void createGroupProductMessages(Map<String, String> productMap, String code) {

		BasicDBObject message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.code");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Product Name  ")));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.description1");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Description (paragraph)")));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.description2");
		message.put("translations", new BasicDBObject("en_GB", createBulletPoints(productMap.get("Description (bullet points)"))));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.material");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Material")));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.technology");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Technology")));
		insertMessage(message);

		message = new BasicDBObject();
		message.put("type", "text");
		message.put("code", code);

		message.put("tag", "product.care");
		message.put("translations", new BasicDBObject("en_GB", productMap.get("Care")));
		insertMessage(message);

	}
	
	private String createBulletPoints(String src){
		String dest = "<ul><li>";
		dest = dest + src.replaceAll("\n", "</li><li>");
		return dest+"</li></ul>";
	}

	private String getCode(String message, String tag) {
		DBCollection dBCollection = getCollection("messages", mongoCmsTemplate);
		QueryBuilder qb = new QueryBuilder();
		qb.and("tag").is(tag);
		qb.and("translations.en").is(message);
		qb.and("type").is("text");
		DBObject dbo = dBCollection.findOne(qb.get());
		if (dbo != null) {
			return (String) dbo.get("code");
		} else {
			System.out.println(message);
			return "";
		}

	}

	private void importProduct(Map<String, String> productMap, String groupCode) {
		BasicDBObject product = new BasicDBObject();

		product.put("type", "simple");
		product.put("status", "PUBLISHED");
		product.put("index", true);
		product.put("visible", true);

		String sort = productMap.get("NAV 3 Sequence");
		
		if(sort.length() == 1)		
			product.put("sort", "000" + sort);
		else if(sort.length() == 2)		
			product.put("sort", "00" + sort);
		else if(sort.length() == 3)		
			product.put("sort", "0" + sort);
		else
			product.put("sort", sort);
		
		product.put("logo", "null");
		product.put("start_date", new Date());
		product.put("assets", new BasicDBObject());
		product.put("style", productMap.get("Style Number [SKU number]"));
	
		BasicDBList categories = new BasicDBList();

		categories.add(productMap.get("NAV 1").trim().toLowerCase().replace(" ", "_"));
		categories.add(productMap.get("NAV 1").trim().toLowerCase().replace(" ", "_") + "." + productMap.get("NAV 2").trim().toLowerCase().replace(" ", "_"));
		categories.add(productMap.get("NAV 1").trim().toLowerCase().replace(" ", "_") + "." + productMap.get("NAV 2").trim().toLowerCase().replace(" ", "_") + "."
				+ productMap.get("NAV 3").trim().toLowerCase().replace(" ", "_"));
		
		BasicDBObject tags = new BasicDBObject();
		
		if(!productMap.get("Editor's Pick").equals("")){
			BasicDBList tmpList = new BasicDBList();
			tmpList.add(productMap.get("NAV 1").trim().toLowerCase().replace(" ", "_"));
			tmpList.add(productMap.get("NAV 1").trim().toLowerCase().replace(" ", "_") + "." + productMap.get("NAV 2").trim().toLowerCase().replace(" ", "_"));
			tmpList.add(productMap.get("NAV 1").trim().toLowerCase().replace(" ", "_") + "." + productMap.get("NAV 2").trim().toLowerCase().replace(" ", "_") + "."
					+ productMap.get("NAV 3").trim().toLowerCase().replace(" ", "_"));
			tags.put("editors_pick", tmpList);
		}
		
		if(!productMap.get("Collection").equals("")){
			BasicDBList tmpList = new BasicDBList();
			tmpList.add(productMap.get("Collection").toLowerCase().replace(" ", "_"));
			tags.put("collection", tmpList);
		}
		
		product.put("tags", tags);
		
		BasicDBObject logos = new BasicDBObject();
		
		if(!productMap.get("Technical Logos").equals("")){
			BasicDBList tmpList = new BasicDBList();
			
			String[] _logos = productMap.get("Technical Logos").split(",");
			
			for(int i = 0; i < _logos.length; i++ ){
				tmpList.add(_logos[i].trim().toLowerCase().replace(" ", "_"));
			}
			logos.put("technical", tmpList);
		}
		
		product.put("logos", logos);


		product.put("categories", categories);
		product.put("main_category", productMap.get("NAV 1").trim().toLowerCase() + "." + productMap.get("NAV 2").trim().toLowerCase() + "." + productMap.get("NAV 3").trim().toLowerCase());

		product.put("gtin", productMap.get("ERN Code [EAN code]"));

		BasicDBList prices = new BasicDBList();

		Double priceUSD = new Double(productMap.get("Price USD "));

		BasicDBObject priceObjectUSD = new BasicDBObject();
		priceObjectUSD.put("group", "default");
		priceObjectUSD.put("start", new Date());
		priceObjectUSD.put("currency", "USD");
		priceObjectUSD.put("value", priceUSD);

		prices.add(priceObjectUSD);

		Double priceEUR = new Double(productMap.get("Price EUR"));

		BasicDBObject priceObjectEUR = new BasicDBObject();
		priceObjectEUR.put("group", "default");
		priceObjectEUR.put("start", new Date());
		priceObjectEUR.put("currency", "EUR");
		priceObjectEUR.put("value", priceEUR);

		prices.add(priceObjectEUR);

		Double priceGBP = new Double(productMap.get("Price GBP"));

		BasicDBObject priceObjectGBP = new BasicDBObject();
		priceObjectGBP.put("group", "default");
		priceObjectGBP.put("start", new Date());
		priceObjectGBP.put("currency", "GBP");
		priceObjectGBP.put("value", priceGBP);

		prices.add(priceObjectGBP);

		product.put("prices", prices);
		product.put("std_prices", prices);

		product.put("tax_codes", new BasicDBObject("UK", "NORMAL"));

		product.put("size", getCode(productMap.get("Sizes"), "size"));
		product.put("color", getCode(productMap.get("Colour"), "color"));
		product.put("colorpicker", getCode(productMap.get("Colour Swatch"), "colorpicker"));

		product.put("primary_group", groupCode);
		product.put("secondary_group", productMap.get("Grouped Products"));

		if (importSimpleProduct) {
			DBCollection dBCollection = getCollection("product_published", mongoShopTemplate);

			QueryBuilder qb = QueryBuilder.start();
			qb.and("gtin").is(productMap.get("ERN Code [EAN code]"));

			dBCollection.update(qb.get(), product, true, false);
		}
		
		BasicDBObject inventory = new BasicDBObject();
		inventory.put("gtin", productMap.get("ERN Code [EAN code]"));
		inventory.put("stock", 10);
		inventory.put("out_of_stock_limit", 0);
		inventory.put("created_at", new Date());
		inventory.put("updated_at", new Date());
		
		if (importSimpleProductInventory) {
			DBCollection dBCollection = getCollection("inventory", mongoShopTemplate);

			QueryBuilder qb = QueryBuilder.start();
			qb.and("gtin").is(productMap.get("ERN Code [EAN code]"));

			dBCollection.update(qb.get(), inventory, true, false);
		}
		
		if(!productMap.get("Wear it with").equals("")){
			BasicDBObject tmp = new BasicDBObject();
			tmp.put("primary_group", groupCode);
			tmp.put("secondary_group", productMap.get("Grouped Products"));
			
			String[] _gtins =  productMap.get("Wear it with").split("\n");
			
			BasicDBList gtins = new BasicDBList();
			
			for(int i = 0; i < _gtins.length; i++ ){
				gtins.add(_gtins[i]);
			}
			
			tmp.put("gtins", gtins);
			
			if (importSimpleProductRecommendations) {
				DBCollection dBCollection = getCollection("product_recommendations", mongoShopTemplate);

				QueryBuilder qb = QueryBuilder.start();
				qb.and("primary_group").is(groupCode);
				qb.and("secondary_group").is(productMap.get("Grouped Products"));

				dBCollection.update(qb.get(), tmp, true, false);
			}
		}

		createProductMessages(productMap, productMap.get("ERN Code [EAN code]"));

	}

	private void importGroupProduct(List<Map<String, String>> groupProductList, String groupCode) {

		BasicDBObject product = new BasicDBObject();
		product.put("code", groupCode);
		product.put("template", "product-detail");

		for (Map<String, String> map : groupProductList) {
			
			importProduct(map, groupCode);
		}

		product.put("dim1", "size");
		product.put("dim2", "color");

		if (importGroupProduct) {
			QueryBuilder qb2 = QueryBuilder.start();
			qb2.and("code").is(groupCode);
			getCollection("group_published", mongoShopTemplate).update(qb2.get(), product, true, false);
		}

		BasicDBObject url = new BasicDBObject();
		url.put("action", "handle");
		url.put("page_id", "product-detail");
		url.put("url_id", "product-detail-" + groupCode);

		BasicDBObject data = new BasicDBObject();
		data.put("primary_group", groupCode);
		data.put("new", 1);
		url.put("data", data);

		BasicDBObject _url = new BasicDBObject();
		_url.put("value", "/product/" + groupProductList.get(0).get("Product Name  ").toLowerCase().replace(" ", "-") + "-" + groupCode.toLowerCase() + ".html");
		_url.put("lang", "en");
		_url.put("shop_code", "PERFECT_MOMENT");
		BasicDBList list = new BasicDBList();
		list.add(_url);
		url.put("urls", list);

		if (importUrl) {
			QueryBuilder qb1 = QueryBuilder.start();
			qb1.and("url_id").is("product-detail-" + groupCode);

			getCollection("urlMapping", mongoCmsTemplate).update(qb1.get(), url, true, false);
		}
		createGroupProductMessages(groupProductList.get(0), groupCode);

	}
	
	public void updateProductAssetsBySecondaryGroup(String liveCollectionCode, String secondaryGroup, Assets assets) {
		try {
			DBObject q = new BasicDBObject("secondary_group", secondaryGroup.toUpperCase());
			DBObject u = new BasicDBObject("$set", new BasicDBObject("assets", encodeAssets(assets)));
			
			DBCollection dBCollection = getCollection("product_published", mongoShopTemplate);
			dBCollection.update(q, u, false, true, WriteConcern.JOURNAL_SAFE);			
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		}
	}

	private Map<String, String> toMap(String[] keys, String[] values) {
		HashMap<String, String> result = new HashMap<String, String>();
		for (int i = 0; i < keys.length; i++) {
			result.put(keys[i], values[i]);
		}
		return result;
	}

	/**
	 * @return the mongoCmsTemplate
	 */
	public MongoTemplate getMongoCmsTemplate() {
		return mongoCmsTemplate;
	}

	/**
	 * @param mongoCmsTemplate
	 *            the mongoCmsTemplate to set
	 */
	public void setMongoCmsTemplate(MongoTemplate mongoCmsTemplate) {
		this.mongoCmsTemplate = mongoCmsTemplate;
	}

	/**
	 * @return the mongoShopTemplate
	 */
	public MongoTemplate getMongoShopTemplate() {
		return mongoShopTemplate;
	}

	/**
	 * @param mongoShopTemplate
	 *            the mongoShopTemplate to set
	 */
	public void setMongoShopTemplate(MongoTemplate mongoShopTemplate) {
		this.mongoShopTemplate = mongoShopTemplate;
	}
	
	private DBObject encodeAssets(Assets assets) {
		DBObject assetsDbo = new BasicDBObject();
		
		for (String tag : assets.keySet()) {
			List<AssetGroup> assetGroups = assets.get(tag);
			BasicDBList assetGroupsDbo = new BasicDBList();
			for (AssetGroup assetGroup : assetGroups) {
				DBObject assetGroupDbo = encodeAssetGroup(assetGroup);
				assetGroupsDbo.add(assetGroupDbo);
			}
			assetsDbo.put(tag, assetGroupsDbo);
		}
		
		return assetsDbo;
	}

	private DBObject encodeAssetGroup(AssetGroup assetGroup) {
		DBObject assetGroupDbo = new BasicDBObject();
		
		for (String tag : assetGroup.keySet()) {
			Asset asset = assetGroup.get(tag);
			DBObject assetDbo = encodeAsset(asset);
			assetGroupDbo.put(tag, assetDbo);
		}
		
		return assetGroupDbo;
	}
	
	private DBObject encodeAsset(Asset asset) {
		DBObject dbo = new BasicDBObject();
		
		dbo.put("type", asset.getType());
		
		dbo.put("path", asset.getPath());
		
		DBObject alt = new BasicDBObject();
		for (Locale locale : asset.getAlt().keySet()) {
			alt.put(locale.toString(), asset.getAlt().get(locale));
		}
		
		return dbo;
	}

	public void csvPriceUpdate(String productCsvPath) {
		try {

			CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(productCsvPath)), ';', '\"',
					CSVReader.DEFAULT_ESCAPE_CHARACTER);
			List<String[]> list = reader.readAll();
			reader.close();

			String[] keys = list.get(0);
			list.remove(0);

			Map<String, List<Map<String, String>>> groups = new HashMap<String, List<Map<String, String>>>();
			for (String[] values : list) {
				try {
					Map<String, String> productMap = toMap(keys, values);
					
					String gtin = productMap.get("ERN Code [EAN code]");
					System.out.println(gtin);
					
					BasicDBList prices = new BasicDBList();

					Double priceUSD = new Double(productMap.get("Price USD "));

					BasicDBObject priceObjectUSD = new BasicDBObject();
					priceObjectUSD.put("group", "default");
					priceObjectUSD.put("start", new Date());
					priceObjectUSD.put("currency", "USD");
					priceObjectUSD.put("value", priceUSD);

					prices.add(priceObjectUSD);

					Double priceEUR = new Double(productMap.get("Price EUR"));

					BasicDBObject priceObjectEUR = new BasicDBObject();
					priceObjectEUR.put("group", "default");
					priceObjectEUR.put("start", new Date());
					priceObjectEUR.put("currency", "EUR");
					priceObjectEUR.put("value", priceEUR);

					prices.add(priceObjectEUR);

					Double priceGBP = new Double(productMap.get("Price GBP"));

					BasicDBObject priceObjectGBP = new BasicDBObject();
					priceObjectGBP.put("group", "default");
					priceObjectGBP.put("start", new Date());
					priceObjectGBP.put("currency", "GBP");
					priceObjectGBP.put("value", priceGBP);

					prices.add(priceObjectGBP);

					System.out.println(prices);
					
					DBObject q = new BasicDBObject("gtin", gtin);
					DBObject u = new BasicDBObject("$set", new BasicDBObject("prices", prices));
					DBCollection dBCollection = getCollection("product_published", mongoShopTemplate);
					dBCollection.update(q, u, false, false, WriteConcern.JOURNAL_SAFE);

					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		}
	}

}
